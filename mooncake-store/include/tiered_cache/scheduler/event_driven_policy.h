#pragma once

#include <unordered_map>
#include <vector>

#include "tiered_cache/scheduler/scheduler_policy.h"
#include "types.h"

namespace mooncake {

/**
 * @struct OffloadDecision
 * @brief Result of an Offload decision (fast-tier hit may be copied to a slow
 *        tier, keeping the fast copy).
 */
struct OffloadDecision {
    bool perform = false;
    UUID target_tier;
};

/**
 * @struct OnboardDecision
 * @brief Result of an Onboard decision (slow-tier hit may be moved up to the
 *        fast tier, deleting the slow copy afterwards).
 */
struct OnboardDecision {
    bool perform = false;
    UUID target_tier;
};

/**
 * @class EventDrivenPolicy
 * @brief Optional mixin implemented by event-driven scheduling policies.
 *
 * Every hook has a no-op default so a policy can opt into only the behaviour it
 * needs. The scheduler probes for this capability with
 * `dynamic_cast<EventDrivenPolicy*>(policy)` and routes access/eviction events
 * through it when present. Policies that only implement the periodic
 * `SchedulerPolicy::Decide` path do not derive from this class.
 */
class EventDrivenPolicy {
   public:
    virtual ~EventDrivenPolicy() = default;

    /**
     * @brief Configure the slow (offload target) tier. The fast tier is set via
     *        SchedulerPolicy::SetFastTier. Default is a no-op.
     */
    virtual void SetSlowTier(UUID /*id*/) {}

    /**
     * @brief Called when `key_ctx` was served from the fast (DRAM) tier.
     * @return perform=true requests a copy to target_tier (source retained).
     */
    virtual OffloadDecision Offload(
        const KeyContext& /*key_ctx*/,
        const std::unordered_map<UUID, TierStats>& /*tier_stats*/) {
        return {};
    }

    /**
     * @brief Called when `key_ctx` was served from a slow tier.
     * @return perform=true requests a move to target_tier (source deleted after
     *         the copy succeeds).
     */
    virtual OnboardDecision Onboard(
        const KeyContext& /*key_ctx*/,
        const std::unordered_map<UUID, TierStats>& /*tier_stats*/) {
        return {};
    }

    /**
     * @brief Called from the background evict task to bring the fast tier back
     *        below its watermark.
     * @param target_reclaim_bytes Minimum number of bytes the returned actions
     *        should reclaim from the fast tier.
     * @return A batch of EVICT/MIGRATE actions ordered coldest-first.
     */
    virtual std::vector<SchedAction> Evict(
        const std::unordered_map<UUID, TierStats>& /*tier_stats*/,
        size_t /*target_reclaim_bytes*/) {
        return {};
    }
};

}  // namespace mooncake
