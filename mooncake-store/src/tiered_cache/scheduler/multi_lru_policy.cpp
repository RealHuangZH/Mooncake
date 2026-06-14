#include "tiered_cache/scheduler/multi_lru_policy.h"

#include <algorithm>

#include "tiered_cache/scheduler/tinylfu_stats_collector.h"

namespace mooncake {

namespace {

bool Contains(const std::vector<UUID>& locations, UUID id) {
    return std::find(locations.begin(), locations.end(), id) != locations.end();
}

size_t FreeBytes(const TierStats& stats) {
    return stats.total_capacity_bytes > stats.used_capacity_bytes
               ? stats.total_capacity_bytes - stats.used_capacity_bytes
               : 0;
}

}  // namespace

MultiLRUPolicy::MultiLRUPolicy(Config config) : config_(config) {}

void MultiLRUPolicy::SetFastTier(UUID id) { fast_id_ = id; }

void MultiLRUPolicy::SetSlowTier(UUID id) { slow_id_ = id; }

OffloadDecision MultiLRUPolicy::Offload(
    const KeyContext& key_ctx,
    const std::unordered_map<UUID, TierStats>& tier_stats) {
    if (!collector_ || !fast_id_.has_value() || !slow_id_.has_value()) {
        return {};
    }
    // 1. Hot enough to be worth a slow-tier replica.
    if (collector_->EstimateFrequency(key_ctx.key) <=
        config_.offload_freq_threshold) {
        return {};
    }
    // 2. Not already present on the slow tier.
    if (Contains(key_ctx.current_locations, *slow_id_)) {
        return {};
    }
    // 3. Slow tier has room.
    auto it = tier_stats.find(*slow_id_);
    if (it == tier_stats.end() ||
        FreeBytes(it->second) < std::max<size_t>(key_ctx.size_bytes, 1)) {
        return {};
    }
    return OffloadDecision{true, *slow_id_};
}

OnboardDecision MultiLRUPolicy::Onboard(
    const KeyContext& key_ctx,
    const std::unordered_map<UUID, TierStats>& tier_stats) {
    if (!collector_ || !fast_id_.has_value()) {
        return {};
    }
    // 1. Hot enough to deserve promotion.
    if (collector_->EstimateFrequency(key_ctx.key) <=
        config_.onboard_freq_threshold) {
        return {};
    }
    // 2. Not already present on the fast tier.
    if (Contains(key_ctx.current_locations, *fast_id_)) {
        return {};
    }
    // 3. Fast tier below the admission watermark and has room.
    auto it = tier_stats.find(*fast_id_);
    if (it == tier_stats.end() || it->second.total_capacity_bytes == 0) {
        return {};
    }
    const TierStats& fast = it->second;
    const double usage = static_cast<double>(fast.used_capacity_bytes) /
                         static_cast<double>(fast.total_capacity_bytes);
    if (usage >= config_.onboard_dram_watermark) {
        return {};
    }
    if (FreeBytes(fast) < std::max<size_t>(key_ctx.size_bytes, 1)) {
        return {};
    }
    return OnboardDecision{true, *fast_id_};
}

std::vector<SchedAction> MultiLRUPolicy::Evict(
    const std::unordered_map<UUID, TierStats>& tier_stats,
    size_t target_reclaim_bytes) {
    std::vector<SchedAction> actions;
    static_cast<void>(tier_stats);
    if (!collector_ || !fast_id_.has_value() || target_reclaim_bytes == 0) {
        return actions;
    }

    // Coldest-first candidates (COLD→VERY_HOT, LRU-tail first within a bucket).
    const auto candidates =
        collector_->EvictionCandidates(config_.eviction_scan_limit);

    size_t planned_bytes = 0;
    for (const auto& key : candidates) {
        if (planned_bytes >= target_reclaim_bytes) {
            break;
        }
        std::optional<KeyContext> ctx =
            key_ctx_provider_ ? key_ctx_provider_(key) : std::nullopt;
        if (!ctx.has_value() || ctx->current_locations.empty()) {
            continue;
        }
        // Only act on fast-tier-resident keys; slow-only keys are managed by
        // the storage tier itself (interpretation (a) of doc 2.3.5).
        if (!Contains(ctx->current_locations, *fast_id_)) {
            continue;
        }

        SchedAction action;
        action.key = key;
        action.source_tier_id = *fast_id_;
        const bool has_other_replica = ctx->current_locations.size() > 1;
        if (has_other_replica) {
            // Another replica exists (e.g. an offloaded slow copy): drop the
            // fast copy outright.
            action.type = SchedAction::Type::EVICT;
        } else if (slow_id_.has_value()) {
            // DRAM-only: migrate down to preserve the data.
            action.type = SchedAction::Type::MIGRATE;
            action.target_tier_id = *slow_id_;
        } else {
            // No slow tier to fall back to.
            action.type = SchedAction::Type::EVICT;
        }
        actions.push_back(std::move(action));
        planned_bytes += std::max<size_t>(ctx->size_bytes, 1);
    }
    return actions;
}

}  // namespace mooncake
