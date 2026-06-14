#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include "tiered_cache/scheduler/event_driven_policy.h"
#include "tiered_cache/scheduler/scheduler_policy.h"
#include "types.h"

namespace mooncake {

class MultiLRUStatsCollector;  // Forward declaration (non-owning dependency)

/**
 * @class MultiLRUPolicy
 * @brief Frequency-aware, event-driven tiering policy.
 *
 * Reacts to individual access events rather than reconciling on a fixed period:
 *   - Offload (fast-tier hit): replicate a sufficiently hot key down to the
 *     slow tier so a future eviction can drop the fast copy cheaply.
 *   - Onboard (slow-tier hit): promote a hot key up to the fast tier (the
 *     scheduler deletes the slow copy after the move).
 *   - Evict (background): hand back EVICT/MIGRATE actions in coldest-first
 *     order to pull the fast tier below its watermark.
 *
 * Frequency comes from an injected MultiLRUStatsCollector (TinyLFU). Per-key
 * residency/size — which the bare Evict signature cannot carry — is supplied by
 * an injected KeyContextProvider so the policy keeps ownership of the EVICT vs
 * MIGRATE decision. The periodic SchedulerPolicy::Decide path is intentionally
 * left as the inherited no-op.
 */
class MultiLRUPolicy : public SchedulerPolicy, public EventDrivenPolicy {
   public:
    struct Config {
        // Offload a fast-tier key once its estimate exceeds this (DRAM→SSD).
        uint64_t offload_freq_threshold = 1;
        // Onboard a slow-tier key once its estimate exceeds this (SSD→DRAM).
        uint64_t onboard_freq_threshold = 3;
        // Refuse onboarding while fast-tier usage is at/above this ratio.
        double onboard_dram_watermark = 0.80;
        // Upper bound on candidates scanned per background eviction pass.
        size_t eviction_scan_limit = 4096;
    };

    using KeyContextProvider =
        std::function<std::optional<KeyContext>(std::string_view)>;

    explicit MultiLRUPolicy(Config config);

    void SetCollector(MultiLRUStatsCollector* collector) {
        collector_ = collector;
    }
    void SetKeyContextProvider(KeyContextProvider provider) {
        key_ctx_provider_ = std::move(provider);
    }

    void SetFastTier(UUID id) override;
    void SetSlowTier(UUID id) override;

    // --- EventDrivenPolicy hooks ---
    OffloadDecision Offload(
        const KeyContext& key_ctx,
        const std::unordered_map<UUID, TierStats>& tier_stats) override;
    OnboardDecision Onboard(
        const KeyContext& key_ctx,
        const std::unordered_map<UUID, TierStats>& tier_stats) override;
    std::vector<SchedAction> Evict(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        size_t target_reclaim_bytes) override;

   private:
    Config config_;
    MultiLRUStatsCollector* collector_ = nullptr;  // non-owning
    KeyContextProvider key_ctx_provider_;
    std::optional<UUID> fast_id_;
    std::optional<UUID> slow_id_;
};

}  // namespace mooncake
