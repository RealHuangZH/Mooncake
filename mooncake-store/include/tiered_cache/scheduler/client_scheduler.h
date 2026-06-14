#pragma once

#include <array>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string_view>
#include <boost/functional/hash.hpp>
#include "mutex.h"
#include "thread_pool.h"
#include "tiered_cache/scheduler/bounded_dedup_queue.h"
#include "tiered_cache/scheduler/event_driven_policy.h"
#include "tiered_cache/scheduler/scheduler_events.h"
#include "tiered_cache/scheduler/scheduler_policy.h"
#include "tiered_cache/scheduler/stats_collector.h"
#include "types.h"
#include "utils.h"

#include <json/value.h>

namespace mooncake {

class TieredBackend;             // Forward declaration
class CacheTier;                 // Forward declaration
class MultiLRUStatsCollector;    // Forward declaration (event-driven collector)

/**
 * @class ClientScheduler
 * @brief Coordinates statistics collection, policy execution, and action
 * application.
 */
class ClientScheduler {
   public:
    ClientScheduler(TieredBackend* backend, const Json::Value& config);
    ~ClientScheduler();

    // Lifecycle management
    void Start();
    void Stop();

    // Register a managed tier
    void RegisterTier(CacheTier* tier);

    // Incoming access hook (thread-safe). Records frequency and, for
    // event-driven policies, routes offload/onboard events based on the tier
    // that served the access.
    void OnAccess(const AccessContext& ctx);

    /**
     * @brief Get current hot key statistics for HA recovery prioritization.
     */
    AccessStats GetHotKeyStats() const;

    // Called when a replica is committed or updated
    void OnCommit(const CommitContext& ctx);

    // Called when a key or a replica is deleted
    void OnDelete(const DeleteContext& ctx);

    // Called when allocation fails due to insufficient space
    // Returns true if reclaim freed enough space for an immediate retry
    bool OnAllocationFailure(const AllocationFailureContext& ctx);

   private:
    struct PlannedReclaim {
        struct Step {
            SchedAction action;
            size_t size_bytes = 0;
        };

        std::vector<Step> steps;
        size_t target_reclaim_bytes = 0;
    };

    // Background worker loop
    void WorkerLoop();

    // Execute generated actions
    void ExecuteActions(const std::vector<SchedAction>& actions);

    // Trigger immediate eviction for a tier (sync mode)
    bool TriggerSyncEviction(UUID tier_id, size_t required_bytes);

    // Reclaim pre-replicated cold replicas without copying data on the
    // allocation failure path.
    bool TryFastReclaim(UUID tier_id, size_t required_bytes);

    PlannedReclaim BuildReclaimPlan(
        UUID tier_id, const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys,
        bool require_existing_replica, size_t required_bytes) const;

    size_t ExecuteReclaimPlan(const PlannedReclaim& plan);
    bool HasAvailableBytes(UUID tier_id, size_t required_bytes) const;
    std::optional<UUID> SelectDemotionTier(UUID source_tier_id) const;

    // Build policy input from the latest stats snapshot and scheduler cache
    std::vector<KeyContext> BuildActiveKeys(
        const AccessStats& access_stats,
        std::optional<UUID> pinned_tier_id = std::nullopt);

    // Build a fresh tier stats map for policy execution
    std::unordered_map<UUID, TierStats> CollectTierStats() const;

    struct CachedKeyState {
        size_t size_bytes = 0;
        std::vector<UUID> current_locations;
    };

    struct KeyCacheShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, CachedKeyState, StringHash,
                           std::equal_to<>>
            key_cache GUARDED_BY(mutex);
        std::unordered_map<
            UUID, std::unordered_set<std::string, StringHash, std::equal_to<>>,
            boost::hash<UUID>>
            tier_resident_keys GUARDED_BY(mutex);
    };

    static constexpr size_t kKeyCacheShardCount = 16;

    static size_t KeyCacheShardIndex(std::string_view key);
    KeyCacheShard& GetKeyCacheShard(std::string_view key);
    const KeyCacheShard& GetKeyCacheShard(std::string_view key) const;

    size_t EstimateActiveKeyReserve(const AccessStats& access_stats,
                                    std::optional<UUID> pinned_tier_id) const;
    void AppendHotKeys(const AccessStats& access_stats,
                       std::vector<KeyContext>& active_keys,
                       std::unordered_set<std::string>& seen_keys) const;
    void AppendPinnedTierKeys(UUID pinned_tier_id,
                              std::vector<KeyContext>& active_keys,
                              std::unordered_set<std::string>& seen_keys) const;
    std::optional<KeyContext> BuildKeyContextLocked(
        const std::string& key, const CachedKeyState& state,
        const AccessStats& access_stats,
        const AccessStatEntry* stat_entry = nullptr) const;
    size_t GetCachedKeySize(const std::string& key) const;

    void TrackReplicaLocked(KeyCacheShard& shard, std::string_view key,
                            UUID tier_id, size_t size_bytes)
        REQUIRES(shard.mutex);
    bool RemoveReplicaLocked(KeyCacheShard& shard, std::string_view key,
                             std::optional<UUID> tier_id) REQUIRES(shard.mutex);

    // --- Event-driven (MULTI_LRU) asynchronous offload/onboard ---

    // True when the active policy implements the EventDrivenPolicy mixin and
    // the async infrastructure has been built.
    bool EventDrivenEnabled() const { return event_policy_ != nullptr; }

    // Admit an offload/onboard event for `key` and dispatch it to the shared
    // pool. No-ops if not event-driven or if the key is already in flight /
    // the queue is full (back-pressure drops the event).
    void EnqueueOffload(std::string_view key);
    void EnqueueOnboard(std::string_view key);

    // Pool-thread consumers. They ask the policy whether to act, re-validate
    // the key's current location/version, then perform the copy/move. All I/O
    // is record_access=false so background movement never feeds the heat stats.
    void ProcessOffload(std::string key);
    void ProcessOnboard(std::string key);

    // Build a minimal KeyContext (key + current locations + size) from the
    // scheduler's metadata cache, for feeding the event-driven policy.
    std::optional<KeyContext> SnapshotKeyContext(std::string_view key) const;

    // One periodic background-eviction step for event-driven mode: recompute
    // the dynamic watermark from write load, and (if over watermark) post a
    // rate-limited eviction task to the shared pool.
    void RunBackgroundEvictCycle();

    // Float the evict watermark within [floor, limit) by write pressure: low
    // write load → higher watermark (let the cache fill), high write load →
    // lower watermark (evict earlier to absorb incoming writes).
    double ComputeDynamicEvictWatermark(double write_rate_bytes_per_sec,
                                        size_t capacity_bytes) const;

    // Proportional per-cycle reclaim budget, clamped to [rate_min, rate_max] of
    // capacity and never below the low watermark.
    size_t ComputeReclaimTarget(double usage_ratio, double dynamic_watermark,
                                size_t capacity_bytes, size_t used_bytes) const;

   private:
    TieredBackend* backend_;
    std::unique_ptr<SchedulerPolicy> policy_;
    std::unique_ptr<StatsCollector> stats_collector_;

    std::atomic<bool> running_{false};
    std::thread worker_thread_;

    // Local view of tiers for policy input
    std::unordered_map<UUID, CacheTier*> tiers_;

    // Scheduler-side metadata cache to avoid full backend scans each cycle
    std::array<KeyCacheShard, kKeyCacheShardCount> key_cache_shards_;
    std::optional<UUID> fast_tier_id_;
    std::optional<UUID> slow_tier_id_;

    // Configuration
    int loop_interval_ms_ = 1000;
    size_t stats_snapshot_limit_ = detail::DefaultSnapshotLimit();
    enum class EvictionMode { SYNC, ASYNC };
    EvictionMode eviction_mode_ = EvictionMode::ASYNC;

    // --- Event-driven (MULTI_LRU) async infrastructure ---
    // Non-owning view of policy_ when it implements the mixin; nullptr means
    // the periodic Decide() path is used instead. multi_lru_collector_ is a
    // non-owning view of stats_collector_ for the same case.
    EventDrivenPolicy* event_policy_ = nullptr;
    MultiLRUStatsCollector* multi_lru_collector_ = nullptr;
    std::unique_ptr<ThreadPool> offload_onboard_pool_;
    std::unique_ptr<BoundedDedupQueue> offload_inflight_;
    std::unique_ptr<BoundedDedupQueue> onboard_inflight_;
    size_t async_pool_threads_ = 2;
    size_t offload_queue_capacity_ = 4096;
    size_t onboard_queue_capacity_ = 4096;

    // Background eviction watermarks/rate (event-driven mode only).
    double evict_watermark_ = 0.90;        // float upper bound (trigger)
    double evict_watermark_floor_ = 0.80;  // float lower bound
    double limit_watermark_ = 0.95;        // hard ceiling (never reached)
    double low_watermark_ = 0.70;          // reclaim target / rate baseline
    double evict_rate_min_ = 0.01;         // min per-cycle reclaim (cap frac)
    double evict_rate_max_ = 0.10;         // max per-cycle reclaim (cap frac)
    double evict_rate_gain_ = 1.0;         // proportional gain k
    // Fast-tier bytes committed in the current cycle; drives the dynamic
    // watermark. Read-and-reset by the worker each cycle.
    std::atomic<size_t> committed_bytes_this_cycle_{0};
    // At most one eviction task in flight on the shared pool at a time.
    std::atomic<bool> evict_in_flight_{false};

    // Used to wake the worker thread immediately on Stop().
    std::mutex cv_mutex_;
    std::condition_variable cv_;
};

}  // namespace mooncake
