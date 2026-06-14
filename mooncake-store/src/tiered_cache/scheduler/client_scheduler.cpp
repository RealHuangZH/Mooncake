#include "tiered_cache/scheduler/client_scheduler.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/scheduler/lru_policy.h"
#include "tiered_cache/scheduler/lru_stats_collector.h"
#include "tiered_cache/scheduler/multi_lru_policy.h"
#include "tiered_cache/scheduler/simple_policy.h"
#include "tiered_cache/scheduler/tinylfu_stats_collector.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <functional>
#include <unordered_set>
#include <glog/logging.h>

namespace mooncake {

ClientScheduler::ClientScheduler(TieredBackend* backend,
                                 const Json::Value& config)
    : backend_(backend) {
    std::string policy_type = "SIMPLE";  // Default
    size_t stats_shards = detail::DefaultStatsShardCount();
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("policy")) {
        policy_type = config["scheduler"]["policy"].asString();
    }

    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("stats_shards")) {
        const auto configured_shards =
            config["scheduler"]["stats_shards"].asUInt64();
        if (configured_shards > 0) {
            stats_shards = static_cast<size_t>(configured_shards);
        }
    }

    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("stats_snapshot_limit")) {
        const auto configured_limit =
            config["scheduler"]["stats_snapshot_limit"].asUInt64();
        stats_snapshot_limit_ = static_cast<size_t>(configured_limit);
    }

    // Read eviction mode configuration
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("eviction_mode")) {
        std::string mode = config["scheduler"]["eviction_mode"].asString();
        if (mode == "sync") {
            eviction_mode_ = EvictionMode::SYNC;
            LOG(INFO) << "Eviction mode: SYNC (immediate)";
        } else {
            eviction_mode_ = EvictionMode::ASYNC;
            LOG(INFO) << "Eviction mode: ASYNC (periodic)";
        }
    }

    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("loop_interval_ms")) {
        const auto v = config["scheduler"]["loop_interval_ms"].asInt();
        if (v > 0) loop_interval_ms_ = v;
    }

    if (config.isMember("scheduler")) {
        const auto& sched = config["scheduler"];
        if (sched.isMember("async_pool_threads")) {
            const auto v = sched["async_pool_threads"].asUInt64();
            if (v > 0) async_pool_threads_ = static_cast<size_t>(v);
        }
        if (sched.isMember("offload_queue_capacity")) {
            const auto v = sched["offload_queue_capacity"].asUInt64();
            if (v > 0) offload_queue_capacity_ = static_cast<size_t>(v);
        }
        if (sched.isMember("onboard_queue_capacity")) {
            const auto v = sched["onboard_queue_capacity"].asUInt64();
            if (v > 0) onboard_queue_capacity_ = static_cast<size_t>(v);
        }
    }

    if (policy_type == "MULTI_LRU") {
        const auto& sched = config["scheduler"];

        // Frequency-aware stats collector (TinyLFU + 4-level MultiLRU).
        MultiLRUStatsCollector::Config collector_cfg;
        collector_cfg.shard_count = stats_shards;
        collector_cfg.max_snapshot_keys = stats_snapshot_limit_;
        if (sched.isMember("tinylfu_sample_window")) {
            const auto v = sched["tinylfu_sample_window"].asUInt64();
            if (v > 0) collector_cfg.sample_window = static_cast<size_t>(v);
        }
        constexpr size_t kBuckets = MultiLRUStatsCollector::kNumBuckets;
        if (sched.isMember("multi_lru_thresholds") &&
            sched["multi_lru_thresholds"].isArray() &&
            sched["multi_lru_thresholds"].size() == kBuckets) {
            for (Json::ArrayIndex i = 0; i < kBuckets; ++i) {
                collector_cfg.thresholds[i] =
                    sched["multi_lru_thresholds"][i].asUInt64();
            }
        }
        auto collector =
            std::make_unique<MultiLRUStatsCollector>(collector_cfg);
        multi_lru_collector_ = collector.get();
        stats_collector_ = std::move(collector);

        // Event-driven policy fed by the collector + a residency provider.
        MultiLRUPolicy::Config policy_cfg;
        if (sched.isMember("offload_freq_threshold")) {
            policy_cfg.offload_freq_threshold =
                sched["offload_freq_threshold"].asUInt64();
        }
        if (sched.isMember("onboard_freq_threshold")) {
            policy_cfg.onboard_freq_threshold =
                sched["onboard_freq_threshold"].asUInt64();
        }
        if (sched.isMember("onboard_dram_watermark")) {
            policy_cfg.onboard_dram_watermark =
                sched["onboard_dram_watermark"].asDouble();
        }
        auto multi_policy = std::make_unique<MultiLRUPolicy>(policy_cfg);
        multi_policy->SetCollector(multi_lru_collector_);
        multi_policy->SetKeyContextProvider(
            [this](std::string_view key) { return SnapshotKeyContext(key); });
        policy_ = std::move(multi_policy);

        // Background eviction watermarks / rate limiting.
        if (sched.isMember("evict_watermark")) {
            evict_watermark_ = sched["evict_watermark"].asDouble();
        }
        if (sched.isMember("evict_watermark_floor")) {
            evict_watermark_floor_ = sched["evict_watermark_floor"].asDouble();
        }
        if (sched.isMember("limit_watermark")) {
            limit_watermark_ = sched["limit_watermark"].asDouble();
        }
        if (sched.isMember("low_watermark")) {
            low_watermark_ = sched["low_watermark"].asDouble();
        }
        if (sched.isMember("evict_rate_min")) {
            evict_rate_min_ = sched["evict_rate_min"].asDouble();
        }
        if (sched.isMember("evict_rate_max")) {
            evict_rate_max_ = sched["evict_rate_max"].asDouble();
        }
        if (sched.isMember("evict_rate_gain")) {
            evict_rate_gain_ = sched["evict_rate_gain"].asDouble();
        }
        // Sanity: low <= floor <= watermark < limit (keep clamp ranges valid).
        const double below_limit = std::nextafter(limit_watermark_, 0.0);
        evict_watermark_floor_ =
            std::clamp(evict_watermark_floor_, 0.0, below_limit);
        evict_watermark_ =
            std::clamp(evict_watermark_, evict_watermark_floor_, below_limit);
        low_watermark_ =
            std::clamp(low_watermark_, 0.0, evict_watermark_floor_);
        LOG(INFO) << "ClientScheduler initialized with MultiLRU Policy"
                  << " (evict_watermark=" << evict_watermark_
                  << ", floor=" << evict_watermark_floor_
                  << ", limit=" << limit_watermark_
                  << ", low=" << low_watermark_ << ")";
    } else if (policy_type == "LRU") {
        // Initialize LRU components
        stats_collector_ = std::make_unique<LRUStatsCollector>(
            stats_shards, stats_snapshot_limit_);

        // LRU Policy Configuration
        LRUPolicy::Config lru_config;
        if (config.isMember("scheduler")) {
            const auto& sched_conf = config["scheduler"];
            if (sched_conf.isMember("high_watermark"))
                lru_config.high_watermark =
                    sched_conf["high_watermark"].asDouble();
            if (sched_conf.isMember("low_watermark"))
                lru_config.low_watermark =
                    sched_conf["low_watermark"].asDouble();
        }

        auto lru_policy = std::make_unique<LRUPolicy>(lru_config);
        policy_ = std::move(lru_policy);
        LOG(INFO) << "ClientScheduler initialized with LRU Policy";
    } else {
        // Default: SIMPLE
        stats_collector_ = std::make_unique<SimpleStatsCollector>(
            0.5, stats_shards, stats_snapshot_limit_);

        // Simple Policy Configuration
        SimplePolicy::Config simple_config;
        simple_config.promotion_threshold =
            5.0;  // Default to 5.0 for tests (matches previous hardcoded value)
        if (config.isMember("scheduler")) {
            const auto& sched_conf = config["scheduler"];
            if (sched_conf.isMember("promotion_threshold"))
                simple_config.promotion_threshold =
                    sched_conf["promotion_threshold"].asDouble();
        }

        auto simple_policy = std::make_unique<SimplePolicy>(simple_config);
        policy_ = std::move(simple_policy);
        LOG(INFO) << "ClientScheduler initialized with Simple Policy";
    }

    // Detect whether the policy is event-driven (implements the
    // EventDrivenPolicy mixin). If so, build the shared async pool and the
    // bounded de-dup queues that back offload/onboard. Periodic-only policies
    // (Simple/LRU) leave this infrastructure null.
    event_policy_ = dynamic_cast<EventDrivenPolicy*>(policy_.get());
    if (event_policy_) {
        offload_onboard_pool_ =
            std::make_unique<ThreadPool>(async_pool_threads_);
        offload_inflight_ =
            std::make_unique<BoundedDedupQueue>(offload_queue_capacity_);
        onboard_inflight_ =
            std::make_unique<BoundedDedupQueue>(onboard_queue_capacity_);
        LOG(INFO) << "ClientScheduler async offload/onboard pool started with "
                  << async_pool_threads_ << " thread(s), offload_cap="
                  << offload_queue_capacity_
                  << ", onboard_cap=" << onboard_queue_capacity_;
    }
}

ClientScheduler::~ClientScheduler() { Stop(); }

void ClientScheduler::RegisterTier(CacheTier* tier) {
    tiers_[tier->GetTierId()] = tier;

    // Auto-Configuration: If tier is DRAM, set it as Fast Tier
    if (tier->GetMemoryType() == MemoryType::DRAM) {
        if (policy_) {
            fast_tier_id_ = tier->GetTierId();
            policy_->SetFastTier(tier->GetTierId());
            LOG(INFO) << "Set Fast Tier to " << tier->GetTierId();
        }
    }

    // First NVME tier is the offload/onboard slow tier for event-driven mode.
    if (tier->GetMemoryType() == MemoryType::NVME &&
        !slow_tier_id_.has_value()) {
        slow_tier_id_ = tier->GetTierId();
        if (event_policy_) {
            event_policy_->SetSlowTier(tier->GetTierId());
        }
        LOG(INFO) << "Set Slow Tier to " << tier->GetTierId();
    }
}

void ClientScheduler::Start() {
    if (running_) return;
    running_ = true;
    worker_thread_ = std::thread(&ClientScheduler::WorkerLoop, this);
}

void ClientScheduler::Stop() {
    running_ = false;
    // Drain and join the async pool BEFORE the worker thread so no offload/
    // onboard task can touch the backend after teardown begins.
    if (offload_onboard_pool_) {
        offload_onboard_pool_->stop();
    }
    cv_.notify_all();  // Wake the worker thread immediately.
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void ClientScheduler::OnAccess(const AccessContext& ctx) {
    // Only reads (Get hits) count towards frequency. Commits are writes, not
    // cache hits, so they neither bump frequency nor route offload/onboard.
    if (ctx.origin != AccessOrigin::kGet) {
        return;
    }
    if (stats_collector_) {
        stats_collector_->RecordAccess(ctx.key);
    }

    // Event-driven routing: the hook only enqueues (cheap, droppable). The
    // policy decides whether to act and the consumer re-validates before any
    // I/O — see ProcessOffload/ProcessOnboard.
    if (!event_policy_) {
        return;
    }
    switch (ctx.served_tier_type) {
        case MemoryType::DRAM:
            EnqueueOffload(ctx.key);  // hot DRAM key → replicate down to SSD
            break;
        case MemoryType::NVME:
            EnqueueOnboard(ctx.key);  // hot SSD key → promote up to DRAM
            break;
        default:
            break;
    }
}

AccessStats ClientScheduler::GetHotKeyStats() const {
    if (stats_collector_) {
        return stats_collector_->GetSnapshot();
    }
    return {};
}

void ClientScheduler::OnCommit(const CommitContext& ctx) {
    {
        auto& shard = GetKeyCacheShard(ctx.key);
        MutexLocker lock(&shard.mutex);
        TrackReplicaLocked(shard, ctx.key, ctx.tier_id, ctx.size_bytes);
    }
    // Track fast-tier write load for the dynamic evict watermark. fast_tier_id_
    // is fixed during RegisterTier (before any commits), so this read is safe.
    if (event_policy_ && fast_tier_id_.has_value() &&
        ctx.tier_id == *fast_tier_id_) {
        committed_bytes_this_cycle_.fetch_add(ctx.size_bytes,
                                              std::memory_order_relaxed);
    }
}

void ClientScheduler::OnDelete(const DeleteContext& ctx) {
    bool remove_stats = false;
    {
        auto& shard = GetKeyCacheShard(ctx.key);
        MutexLocker lock(&shard.mutex);
        remove_stats = RemoveReplicaLocked(shard, ctx.key, ctx.tier_id);
    }

    if (remove_stats && stats_collector_) {
        stats_collector_->RemoveKey(ctx.key);
    }
}

bool ClientScheduler::OnAllocationFailure(const AllocationFailureContext& ctx) {
    const UUID tier_id = ctx.tier_id;
    const size_t required_bytes = ctx.required_bytes;
    if (TryFastReclaim(tier_id, required_bytes)) {
        LOG(INFO) << "Allocation failed on tier " << tier_id
                  << ", reclaimed pre-replicated cold replicas";
        return true;
    }

    if (eviction_mode_ == EvictionMode::SYNC) {
        LOG(INFO) << "Allocation failed on tier " << tier_id
                  << ", triggering SYNC eviction";
        return TriggerSyncEviction(tier_id, required_bytes);
    } else {
        VLOG(2) << "Allocation failed on tier " << tier_id
                << ", ASYNC mode - will handle in next cycle";
        return false;
    }
}

std::optional<KeyContext> ClientScheduler::SnapshotKeyContext(
    std::string_view key) const {
    const auto& shard = GetKeyCacheShard(key);
    MutexLocker lock(&shard.mutex);
    auto it = shard.key_cache.find(key);
    if (it == shard.key_cache.end() || it->second.current_locations.empty()) {
        return std::nullopt;
    }
    KeyContext ctx;
    ctx.key = std::string(key);
    ctx.current_locations = it->second.current_locations;
    ctx.size_bytes = it->second.size_bytes;
    return ctx;
}

void ClientScheduler::EnqueueOffload(std::string_view key) {
    if (!offload_inflight_ || !offload_onboard_pool_) return;
    if (!offload_inflight_->TryAdmit(key)) return;  // dup or full → drop
    std::string k(key);
    try {
        offload_onboard_pool_->enqueue(
            [this, k]() mutable { ProcessOffload(std::move(k)); });
    } catch (const std::exception&) {
        // Pool already stopped — release the slot we just reserved.
        offload_inflight_->MarkDone(key);
    }
}

void ClientScheduler::EnqueueOnboard(std::string_view key) {
    if (!onboard_inflight_ || !offload_onboard_pool_) return;
    if (!onboard_inflight_->TryAdmit(key)) return;  // dup or full → drop
    std::string k(key);
    try {
        offload_onboard_pool_->enqueue(
            [this, k]() mutable { ProcessOnboard(std::move(k)); });
    } catch (const std::exception&) {
        onboard_inflight_->MarkDone(key);
    }
}

void ClientScheduler::ProcessOffload(std::string key) {
    // Release the in-flight slot on every exit path.
    struct SlotGuard {
        BoundedDedupQueue* queue;
        const std::string& key;
        ~SlotGuard() {
            if (queue) queue->MarkDone(key);
        }
    } guard{offload_inflight_.get(), key};

    if (!running_.load() || !event_policy_ || !backend_ ||
        !fast_tier_id_.has_value()) {
        return;
    }

    auto ctx = SnapshotKeyContext(key);
    if (!ctx.has_value()) return;

    const auto tier_stats = CollectTierStats();
    const OffloadDecision decision = event_policy_->Offload(*ctx, tier_stats);
    if (!decision.perform) return;
    const UUID target = decision.target_tier;

    // Re-validate against the authoritative backend state: the key must still
    // live on the fast tier and must not already have a target replica. This
    // discards events made stale by a concurrent delete/migrate/rewrite.
    if (!backend_->Exist(key, *fast_tier_id_)) return;
    if (backend_->Exist(key, target)) return;

    uint64_t version = 0;
    auto source =
        backend_->Get(key, *fast_tier_id_, /*record_access=*/false, &version);
    if (!source.has_value()) return;

    // offload == REPLICATE: copy to the slow tier with version CAS, keeping
    // the fast copy. A stale version or a full target fails the CAS harmlessly.
    auto res = backend_->CopyData(key, source.value()->loc.data, target,
                                  version, /*record_access=*/false);
    if (!res.has_value() && res.error() != ErrorCode::CAS_FAILED &&
        res.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
        VLOG(2) << "Offload copy failed for key " << key
                << ", error: " << res.error();
    }
}

void ClientScheduler::ProcessOnboard(std::string key) {
    struct SlotGuard {
        BoundedDedupQueue* queue;
        const std::string& key;
        ~SlotGuard() {
            if (queue) queue->MarkDone(key);
        }
    } guard{onboard_inflight_.get(), key};

    if (!running_.load() || !event_policy_ || !backend_ ||
        !fast_tier_id_.has_value() || !slow_tier_id_.has_value()) {
        return;
    }

    auto ctx = SnapshotKeyContext(key);
    if (!ctx.has_value()) return;

    const auto tier_stats = CollectTierStats();
    const OnboardDecision decision = event_policy_->Onboard(*ctx, tier_stats);
    if (!decision.perform) return;
    const UUID fast = decision.target_tier;
    const UUID slow = *slow_tier_id_;

    // Re-validate: still resident on the slow tier and not already promoted.
    if (backend_->Exist(key, fast)) return;
    if (!backend_->Exist(key, slow)) return;

    // onboard == MIGRATE: Transfer copies slow → fast with version CAS; on
    // success we drop the slow replica to complete the move.
    auto res = backend_->Transfer(key, slow, fast, /*record_access=*/false);
    if (!res.has_value()) {
        if (res.error() != ErrorCode::CAS_FAILED &&
            res.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
            VLOG(2) << "Onboard transfer failed for key " << key
                    << ", error: " << res.error();
        }
        return;
    }

    auto del = backend_->Delete(key, slow);
    if (!del.has_value() && del.error() != ErrorCode::OBJECT_NOT_FOUND &&
        del.error() != ErrorCode::TIER_NOT_FOUND) {
        VLOG(2) << "Onboard source cleanup failed for key " << key
                << ", error: " << del.error();
    }
}

void ClientScheduler::RunBackgroundEvictCycle() {
    if (!event_policy_ || !fast_tier_id_.has_value() ||
        !offload_onboard_pool_) {
        return;
    }

    // 1. Write load for this cycle (bytes/sec) from accumulated fast-tier
    //    commits, read-and-reset atomically.
    const size_t committed =
        committed_bytes_this_cycle_.exchange(0, std::memory_order_relaxed);
    const double interval_s =
        loop_interval_ms_ > 0 ? loop_interval_ms_ / 1000.0 : 1.0;
    const double write_rate = static_cast<double>(committed) / interval_s;

    // 2. Fast-tier usage.
    const auto tier_stats = CollectTierStats();
    auto it = tier_stats.find(*fast_tier_id_);
    if (it == tier_stats.end() || it->second.total_capacity_bytes == 0) {
        return;
    }
    const size_t capacity = it->second.total_capacity_bytes;
    const size_t used = it->second.used_capacity_bytes;
    const double usage =
        static_cast<double>(used) / static_cast<double>(capacity);

    // 3. Dynamic watermark + trigger check.
    const double watermark = ComputeDynamicEvictWatermark(write_rate, capacity);
    if (usage <= watermark) {
        return;
    }

    // 4. Proportional reclaim budget for this cycle.
    const size_t target =
        ComputeReclaimTarget(usage, watermark, capacity, used);
    if (target == 0) {
        return;
    }

    // 5. Post a single eviction task to the shared pool (non-blocking). The
    //    in-flight guard keeps cycles from stacking eviction tasks.
    bool expected = false;
    if (!evict_in_flight_.compare_exchange_strong(expected, true)) {
        return;
    }
    try {
        offload_onboard_pool_->enqueue([this, tier_stats, target]() {
            struct ClearFlag {
                std::atomic<bool>* flag;
                ~ClearFlag() { flag->store(false, std::memory_order_relaxed); }
            } clear_on_exit{&evict_in_flight_};
            if (!running_.load() || !event_policy_) return;
            auto actions = event_policy_->Evict(tier_stats, target);
            if (!actions.empty()) {
                ExecuteActions(actions);
            }
        });
    } catch (const std::exception&) {
        evict_in_flight_.store(false, std::memory_order_relaxed);
    }
}

double ClientScheduler::ComputeDynamicEvictWatermark(
    double write_rate_bytes_per_sec, size_t capacity_bytes) const {
    // Normalize write pressure against a capacity-per-second reference so the
    // formula needs no workload-specific calibration: writing the whole tier in
    // one second is treated as maximum pressure.
    const double reference =
        capacity_bytes > 0 ? static_cast<double>(capacity_bytes) : 1.0;
    const double pressure =
        std::clamp(write_rate_bytes_per_sec / reference, 0.0, 1.0);
    // Low pressure → watermark near the upper bound; high pressure → floor.
    double watermark = evict_watermark_ -
                       (evict_watermark_ - evict_watermark_floor_) * pressure;
    return std::clamp(watermark, evict_watermark_floor_,
                      std::nextafter(limit_watermark_, 0.0));
}

size_t ClientScheduler::ComputeReclaimTarget(double usage_ratio,
                                             double dynamic_watermark,
                                             size_t capacity_bytes,
                                             size_t used_bytes) const {
    // Proportional control across the "danger zone" between the dynamic
    // watermark and the hard limit: just over the watermark reclaims ~rate_min,
    // approaching the limit reclaims ~rate_max. This avoids one-shot bulk
    // eviction (which would cause usage jitter) while still reacting urgently
    // when close to the ceiling.
    const double danger_span = limit_watermark_ - dynamic_watermark;
    const double danger =
        danger_span > 0.0
            ? std::clamp((usage_ratio - dynamic_watermark) / danger_span, 0.0,
                         1.0)
            : 1.0;
    double fraction = evict_rate_min_ + (evict_rate_max_ - evict_rate_min_) *
                                            evict_rate_gain_ * danger;
    fraction = std::clamp(fraction, evict_rate_min_, evict_rate_max_);

    const size_t target =
        static_cast<size_t>(fraction * static_cast<double>(capacity_bytes));

    // Never reclaim below the low watermark.
    const size_t low_bytes = static_cast<size_t>(
        low_watermark_ * static_cast<double>(capacity_bytes));
    const size_t reclaimable =
        used_bytes > low_bytes ? used_bytes - low_bytes : 0;
    return std::min(target, reclaimable);
}

void ClientScheduler::WorkerLoop() {
    while (running_) {
        // Use condition_variable so Stop() can wake us immediately.
        {
            std::unique_lock<std::mutex> lk(cv_mutex_);
            cv_.wait_for(lk, std::chrono::milliseconds(loop_interval_ms_),
                         [this] { return !running_.load(); });
        }
        if (!running_) break;

        // Event-driven mode reacts to access events; the periodic loop only
        // runs the background eviction controller (watermark + rate limiting).
        if (event_policy_) {
            RunBackgroundEvictCycle();
            continue;
        }

        // 1. Collect Stats
        auto access_stats = stats_collector_->GetSnapshot();

        // 2. Build Policy Context
        auto active_keys = BuildActiveKeys(access_stats, fast_tier_id_);
        if (active_keys.empty()) continue;

        auto tier_stats_map = CollectTierStats();

        // 3. Make Decision
        if (!policy_) {
            LOG(ERROR) << "ClientScheduler worker has no policy configured";
            continue;
        }

        auto decision = policy_->Decide(tier_stats_map, active_keys);
        if (!decision) {
            LOG(ERROR) << "Scheduler policy decide failed, error: "
                       << decision.error();
            continue;
        }
        const auto& actions = decision.value();

        // 4. Execute Actions
        if (!actions.empty()) {
            ExecuteActions(actions);
        }
    }
}

std::unordered_map<UUID, TierStats> ClientScheduler::CollectTierStats() const {
    std::unordered_map<UUID, TierStats> tier_stats_map;
    for (const auto& [id, tier] : tiers_) {
        tier_stats_map[id] = {tier->GetCapacity(), tier->GetUsage()};
    }
    return tier_stats_map;
}

std::vector<KeyContext> ClientScheduler::BuildActiveKeys(
    const AccessStats& access_stats, std::optional<UUID> pinned_tier_id) {
    const size_t reserved_size =
        EstimateActiveKeyReserve(access_stats, pinned_tier_id);
    std::vector<KeyContext> active_keys;
    std::unordered_set<std::string> seen_keys;
    active_keys.reserve(reserved_size);
    seen_keys.reserve(reserved_size);

    AppendHotKeys(access_stats, active_keys, seen_keys);
    if (pinned_tier_id.has_value()) {
        AppendPinnedTierKeys(pinned_tier_id.value(), active_keys, seen_keys);
    }
    return active_keys;
}

size_t ClientScheduler::KeyCacheShardIndex(std::string_view key) {
    return std::hash<std::string_view>{}(key) % kKeyCacheShardCount;
}

ClientScheduler::KeyCacheShard& ClientScheduler::GetKeyCacheShard(
    std::string_view key) {
    return key_cache_shards_[KeyCacheShardIndex(key)];
}

const ClientScheduler::KeyCacheShard& ClientScheduler::GetKeyCacheShard(
    std::string_view key) const {
    return key_cache_shards_[KeyCacheShardIndex(key)];
}

size_t ClientScheduler::EstimateActiveKeyReserve(
    const AccessStats& access_stats, std::optional<UUID> pinned_tier_id) const {
    size_t reserved_size = access_stats.hot_keys.size();
    if (!pinned_tier_id.has_value()) {
        return reserved_size;
    }

    for (const auto& shard : key_cache_shards_) {
        MutexLocker lock(&shard.mutex);
        auto resident_it =
            shard.tier_resident_keys.find(pinned_tier_id.value());
        if (resident_it != shard.tier_resident_keys.end()) {
            reserved_size += resident_it->second.size();
        }
    }

    return reserved_size;
}

void ClientScheduler::AppendHotKeys(
    const AccessStats& access_stats, std::vector<KeyContext>& active_keys,
    std::unordered_set<std::string>& seen_keys) const {
    for (const auto& stat_entry : access_stats.hot_keys) {
        const auto& shard = GetKeyCacheShard(stat_entry.key);
        MutexLocker lock(&shard.mutex);
        auto cache_it = shard.key_cache.find(stat_entry.key);
        if (cache_it == shard.key_cache.end()) {
            continue;
        }

        auto key_ctx = BuildKeyContextLocked(stat_entry.key, cache_it->second,
                                             access_stats, &stat_entry);
        if (!key_ctx.has_value()) {
            continue;
        }

        seen_keys.insert(stat_entry.key);
        active_keys.push_back(std::move(key_ctx.value()));
    }
}

void ClientScheduler::AppendPinnedTierKeys(
    UUID pinned_tier_id, std::vector<KeyContext>& active_keys,
    std::unordered_set<std::string>& seen_keys) const {
    const AccessStats empty_stats{};
    for (const auto& shard : key_cache_shards_) {
        MutexLocker lock(&shard.mutex);
        auto resident_it = shard.tier_resident_keys.find(pinned_tier_id);
        if (resident_it == shard.tier_resident_keys.end()) {
            continue;
        }

        for (const auto& key : resident_it->second) {
            if (seen_keys.count(key) > 0) {
                continue;
            }

            auto cache_it = shard.key_cache.find(key);
            if (cache_it == shard.key_cache.end()) {
                continue;
            }

            auto key_ctx =
                BuildKeyContextLocked(key, cache_it->second, empty_stats);
            if (!key_ctx.has_value()) {
                continue;
            }

            seen_keys.insert(key);
            active_keys.push_back(std::move(key_ctx.value()));
        }
    }
}

std::optional<KeyContext> ClientScheduler::BuildKeyContextLocked(
    const std::string& key, const CachedKeyState& state,
    const AccessStats& access_stats, const AccessStatEntry* stat_entry) const {
    if (state.current_locations.empty()) {
        return std::nullopt;
    }

    KeyContext key_ctx;
    key_ctx.key = key;
    key_ctx.current_locations = state.current_locations;
    key_ctx.size_bytes = state.size_bytes;
    if (stat_entry != nullptr) {
        if (access_stats.metric == AccessStatMetric::kRecentHeat) {
            key_ctx.recent_heat_score = stat_entry->recent_heat_score;
        } else if (access_stats.metric == AccessStatMetric::kRecencyRank) {
            key_ctx.recency_rank = stat_entry->recency_rank;
        }
    }
    return key_ctx;
}

size_t ClientScheduler::GetCachedKeySize(const std::string& key) const {
    const auto& shard = GetKeyCacheShard(key);
    MutexLocker lock(&shard.mutex);
    auto key_it = shard.key_cache.find(key);
    if (key_it == shard.key_cache.end() || key_it->second.size_bytes == 0) {
        return 1;
    }
    return key_it->second.size_bytes;
}

void ClientScheduler::TrackReplicaLocked(KeyCacheShard& shard,
                                         std::string_view key, UUID tier_id,
                                         size_t size_bytes) {
    auto cache_it = shard.key_cache.find(key);
    if (cache_it == shard.key_cache.end()) {
        cache_it =
            shard.key_cache.emplace(std::string(key), CachedKeyState{}).first;
    }
    auto& state = cache_it->second;
    state.size_bytes = size_bytes;

    const auto existing_it = std::find(state.current_locations.begin(),
                                       state.current_locations.end(), tier_id);
    if (existing_it == state.current_locations.end()) {
        state.current_locations.push_back(tier_id);
    }

    auto& tier_set = shard.tier_resident_keys[tier_id];
    if (tier_set.find(key) == tier_set.end()) {
        tier_set.emplace(std::string(key));
    }
}

bool ClientScheduler::RemoveReplicaLocked(KeyCacheShard& shard,
                                          std::string_view key,
                                          std::optional<UUID> tier_id) {
    auto cache_it = shard.key_cache.find(key);
    if (cache_it == shard.key_cache.end()) {
        return true;
    }

    auto erase_resident = [&](auto& resident_set) {
        auto rit = resident_set.find(key);
        if (rit != resident_set.end()) {
            resident_set.erase(rit);
        }
    };

    if (!tier_id.has_value()) {
        for (const auto& resident_tier : cache_it->second.current_locations) {
            auto resident_it = shard.tier_resident_keys.find(resident_tier);
            if (resident_it != shard.tier_resident_keys.end()) {
                erase_resident(resident_it->second);
            }
        }
        shard.key_cache.erase(cache_it);
        return true;
    }

    auto& current_locations = cache_it->second.current_locations;
    current_locations.erase(
        std::remove(current_locations.begin(), current_locations.end(),
                    tier_id.value()),
        current_locations.end());

    auto resident_it = shard.tier_resident_keys.find(tier_id.value());
    if (resident_it != shard.tier_resident_keys.end()) {
        erase_resident(resident_it->second);
    }

    if (current_locations.empty()) {
        shard.key_cache.erase(cache_it);
        return true;
    }
    return false;
}

void ClientScheduler::ExecuteActions(const std::vector<SchedAction>& actions) {
    // Execute in three phases: EVICT first, then MIGRATE, then REPLICATE.
    // This keeps the fast path biased towards freeing space before background
    // copy work.

    // Phase 1: Execute all EVICT actions
    for (const auto& action : actions) {
        if (!running_) return;  // Fast exit on shutdown
        if (action.type == SchedAction::Type::EVICT) {
            if (!action.source_tier_id.has_value()) continue;

            // Execute Eviction (Delete from specific tier)
            auto res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!res) {
                LOG(ERROR) << "Eviction failed for key: " << action.key
                           << ", error: " << res.error();
            } else {
                VLOG(1) << "Evicted key: " << action.key << " from tier "
                        << action.source_tier_id.value();
            }
        }
    }

    // Phase 2: Execute all MIGRATE actions
    std::unordered_map<UUID, size_t> tiers_needing_eviction;

    for (const auto& action : actions) {
        if (!running_) return;  // Fast exit on shutdown
        if (action.type == SchedAction::Type::MIGRATE) {
            // Check validity
            if (!action.source_tier_id.has_value() ||
                !action.target_tier_id.has_value()) {
                continue;
            }

            // Execute Transfer (Migration/Promotion)
            auto res =
                backend_->Transfer(action.key, action.source_tier_id.value(),
                                   action.target_tier_id.value(), false);

            if (!res) {
                // Log error
                if (res.error() == ErrorCode::CAS_FAILED) {
                    LOG(INFO) << "Transfer aborted due to concurrent "
                                 "modification (CAS Failed) for key: "
                              << action.key;
                } else if (res.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                    // Insufficient space - mark tier for eviction
                    const size_t key_size = GetCachedKeySize(action.key);
                    VLOG(2) << "Transfer skipped due to insufficient space for "
                               "key: "
                            << action.key << ", will trigger eviction";
                    auto& required_bytes =
                        tiers_needing_eviction[action.target_tier_id.value()];
                    required_bytes = std::max(required_bytes, key_size);
                } else {
                    LOG(ERROR) << "Transfer failed for key: " << action.key
                               << ", error: " << res.error();
                }
                // For MVP: ignore
            } else {
                // Transfer successful, delete from source (Move semantics)
                auto del_res =
                    backend_->Delete(action.key, action.source_tier_id.value());
                if (!del_res) {
                    // Log warning: Failed to clean up source
                }
            }
        }
    }

    // Phase 3: If any tier ran out of space, handle based on eviction mode
    if (!tiers_needing_eviction.empty()) {
        if (eviction_mode_ == EvictionMode::SYNC) {
            // Sync mode: trigger immediate eviction
            VLOG(1) << "Triggering SYNC eviction for "
                    << tiers_needing_eviction.size() << " tier(s)";
            for (const auto& [tier_id, required_bytes] :
                 tiers_needing_eviction) {
                TriggerSyncEviction(tier_id, required_bytes);
            }
        } else {
            // Async mode: rely on next scheduling cycle
            VLOG(1) << "ASYNC eviction mode: will handle in next cycle for "
                    << tiers_needing_eviction.size() << " tier(s)";
        }
    }

    // Phase 4: Prepare cold replicas in lower tiers without deleting the fast
    // copy. This builds a reclaimable window before the tier reaches the high
    // watermark.
    for (const auto& action : actions) {
        if (action.type != SchedAction::Type::REPLICATE ||
            !action.source_tier_id.has_value() ||
            !action.target_tier_id.has_value()) {
            continue;
        }

        uint64_t start_version = 0;
        auto source_handle = backend_->Get(action.key, action.source_tier_id,
                                           false, &start_version);
        if (!source_handle) {
            if (source_handle.error() != ErrorCode::INVALID_KEY &&
                source_handle.error() != ErrorCode::TIER_NOT_FOUND) {
                LOG(ERROR) << "Failed to prepare replica for key: "
                           << action.key
                           << ", error: " << source_handle.error();
            }
            continue;
        }

        auto copy_res = backend_->CopyData(
            action.key, source_handle.value()->loc.data,
            action.target_tier_id.value(), start_version, false);
        if (!copy_res) {
            if (copy_res.error() == ErrorCode::CAS_FAILED ||
                copy_res.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                VLOG(2) << "Replica preparation skipped for key: " << action.key
                        << ", error: " << copy_res.error();
            } else {
                LOG(ERROR) << "Replica preparation failed for key: "
                           << action.key << ", error: " << copy_res.error();
            }
            continue;
        }

        VLOG(1) << "Prepared reclaimable replica for key: " << action.key
                << " from tier " << action.source_tier_id.value() << " to tier "
                << action.target_tier_id.value();
    }
}

bool ClientScheduler::TriggerSyncEviction(UUID tier_id, size_t required_bytes) {
    auto tier_stats = CollectTierStats();
    auto access_stats = stats_collector_->GetSnapshot();
    auto active_keys = BuildActiveKeys(access_stats, tier_id);
    auto plan = BuildReclaimPlan(tier_id, tier_stats, active_keys, false,
                                 required_bytes);

    if (plan.steps.empty()) {
        LOG(WARNING) << "No sync reclaim candidates found for tier " << tier_id;
        return false;
    }

    const size_t reclaimed_bytes = ExecuteReclaimPlan(plan);
    const bool enough_space = HasAvailableBytes(tier_id, required_bytes);
    if (!enough_space) {
        VLOG(1) << "Sync reclaim on tier " << tier_id << " freed "
                << reclaimed_bytes << " bytes, still short for "
                << required_bytes << " bytes";
    }
    return reclaimed_bytes >= plan.target_reclaim_bytes && enough_space;
}

bool ClientScheduler::TryFastReclaim(UUID tier_id, size_t required_bytes) {
    auto tier_stats = CollectTierStats();
    auto access_stats = stats_collector_->GetSnapshot();
    auto active_keys = BuildActiveKeys(access_stats, tier_id);
    auto plan = BuildReclaimPlan(tier_id, tier_stats, active_keys, true,
                                 required_bytes);
    if (plan.steps.empty()) {
        return false;
    }

    const size_t reclaimed_bytes = ExecuteReclaimPlan(plan);
    const bool enough_space = HasAvailableBytes(tier_id, required_bytes);
    return reclaimed_bytes >= plan.target_reclaim_bytes && enough_space;
}

ClientScheduler::PlannedReclaim ClientScheduler::BuildReclaimPlan(
    UUID tier_id, const std::unordered_map<UUID, TierStats>& tier_stats,
    const std::vector<KeyContext>& active_keys, bool require_existing_replica,
    size_t required_bytes) const {
    PlannedReclaim plan;

    auto tier_it = tier_stats.find(tier_id);
    if (tier_it == tier_stats.end()) {
        return plan;
    }

    const size_t total_capacity = tier_it->second.total_capacity_bytes;
    const size_t used_capacity = tier_it->second.used_capacity_bytes;
    const size_t available_bytes =
        (total_capacity > used_capacity) ? (total_capacity - used_capacity) : 0;
    const size_t reclaim_needed = (required_bytes > available_bytes)
                                      ? (required_bytes - available_bytes)
                                      : 1;

    plan.target_reclaim_bytes = reclaim_needed;
    plan.steps.reserve(active_keys.size());

    const auto demotion_tier_id =
        require_existing_replica ? std::nullopt : SelectDemotionTier(tier_id);

    size_t planned_reclaim_bytes = 0;
    for (auto it = active_keys.rbegin(); it != active_keys.rend(); ++it) {
        const auto tier_pos = std::find(it->current_locations.begin(),
                                        it->current_locations.end(), tier_id);
        if (tier_pos == it->current_locations.end()) {
            continue;
        }

        PlannedReclaim::Step step;
        step.action.key = it->key;
        step.action.source_tier_id = tier_id;
        step.size_bytes = it->size_bytes;

        const bool has_other_replica = it->current_locations.size() > 1;
        if (require_existing_replica) {
            if (!has_other_replica) {
                continue;
            }
            step.action.type = SchedAction::Type::EVICT;
        } else if (has_other_replica) {
            step.action.type = SchedAction::Type::EVICT;
        } else if (demotion_tier_id.has_value()) {
            step.action.type = SchedAction::Type::MIGRATE;
            step.action.target_tier_id = demotion_tier_id.value();
        } else {
            step.action.type = SchedAction::Type::EVICT;
        }

        plan.steps.push_back(std::move(step));
        planned_reclaim_bytes += it->size_bytes;
        if (planned_reclaim_bytes >= plan.target_reclaim_bytes) {
            break;
        }
    }

    return plan;
}

size_t ClientScheduler::ExecuteReclaimPlan(const PlannedReclaim& plan) {
    size_t reclaimed_bytes = 0;

    for (const auto& step : plan.steps) {
        const auto& action = step.action;
        if (!action.source_tier_id.has_value()) {
            continue;
        }

        if (action.type == SchedAction::Type::EVICT) {
            auto delete_res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!delete_res) {
                continue;
            }
        } else if (action.type == SchedAction::Type::MIGRATE) {
            if (!action.target_tier_id.has_value()) {
                continue;
            }

            auto transfer_res =
                backend_->Transfer(action.key, action.source_tier_id.value(),
                                   action.target_tier_id.value(), false);
            if (!transfer_res) {
                continue;
            }

            auto delete_res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!delete_res) {
                continue;
            }
        } else {
            continue;
        }

        reclaimed_bytes += step.size_bytes;
        if (reclaimed_bytes >= plan.target_reclaim_bytes) {
            break;
        }
    }

    return reclaimed_bytes;
}

bool ClientScheduler::HasAvailableBytes(UUID tier_id,
                                        size_t required_bytes) const {
    auto tier_it = tiers_.find(tier_id);
    if (tier_it == tiers_.end() || !tier_it->second) {
        return false;
    }

    const size_t capacity = tier_it->second->GetCapacity();
    const size_t usage = tier_it->second->GetUsage();
    const size_t available_bytes = (capacity > usage) ? (capacity - usage) : 0;
    return available_bytes >= required_bytes;
}

std::optional<UUID> ClientScheduler::SelectDemotionTier(
    UUID source_tier_id) const {
    const auto tier_views = backend_->GetTierViews();
    const TierView* source_view = nullptr;
    for (const auto& tier_view : tier_views) {
        if (tier_view.id == source_tier_id) {
            source_view = &tier_view;
            break;
        }
    }
    if (!source_view) {
        return std::nullopt;
    }

    const TierView* best_lower_priority = nullptr;
    const TierView* best_fallback = nullptr;
    for (const auto& tier_view : tier_views) {
        if (tier_view.id == source_tier_id) {
            continue;
        }

        if (!best_fallback || tier_view.priority > best_fallback->priority) {
            best_fallback = &tier_view;
        }

        if (tier_view.priority >= source_view->priority) {
            continue;
        }

        if (!best_lower_priority ||
            tier_view.priority > best_lower_priority->priority) {
            best_lower_priority = &tier_view;
        }
    }

    if (best_lower_priority) {
        return best_lower_priority->id;
    }
    if (best_fallback) {
        return best_fallback->id;
    }
    return std::nullopt;
}

}  // namespace mooncake
