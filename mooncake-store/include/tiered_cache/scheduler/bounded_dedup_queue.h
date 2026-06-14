#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "utils.h"

namespace mooncake {

/**
 * @class BoundedDedupQueue
 * @brief Bounded, de-duplicating admission controller for asynchronous
 *        offload/onboard work.
 *
 * A key is considered "in flight" from the moment it is admitted until
 * `MarkDone` is called for it. Admission is rejected (the event is dropped)
 * when either:
 *   - the key is already in flight (de-duplication — avoids redundant copies
 *     for a hot key read at high QPS), or
 *   - the number of in-flight keys has reached the configured capacity
 *     (back-pressure — prevents a read storm from unbounded queue growth).
 *
 * Dropped events are intentionally lost; the scheduler's periodic background
 * pass is the safety net that eventually reconciles them. The structure is
 * sharded so the hot enqueue path contends only within a shard.
 */
class BoundedDedupQueue {
   public:
    explicit BoundedDedupQueue(size_t capacity, size_t shard_count = 16)
        : capacity_(capacity == 0 ? 1 : capacity),
          shards_(NormalizeShards(shard_count)),
          shard_mask_(shards_.size() - 1) {}

    /**
     * @brief Try to admit `key`.
     * @return true if newly admitted — the caller now owns the slot and must
     *         release it with MarkDone(key); false if dropped (duplicate or
     *         capacity reached).
     */
    bool TryAdmit(std::string_view key) {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (shard.in_flight.find(key) != shard.in_flight.end()) {
            return false;  // already queued/processing
        }
        if (size_.load(std::memory_order_relaxed) >= capacity_) {
            return false;  // bounded back-pressure
        }
        shard.in_flight.emplace(key);
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    /// Release the slot held by `key` (no-op if it was not in flight).
    void MarkDone(std::string_view key) {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.in_flight.find(key);
        if (it != shard.in_flight.end()) {
            shard.in_flight.erase(it);
            size_.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    size_t InFlightCount() const {
        return size_.load(std::memory_order_relaxed);
    }
    size_t Capacity() const { return capacity_; }

   private:
    struct alignas(64) Shard {
        std::mutex mutex;
        std::unordered_set<std::string, StringHash, std::equal_to<>> in_flight;
    };

    static size_t NormalizeShards(size_t shard_count) {
        size_t normalized = 1;
        while (normalized < std::max<size_t>(1, shard_count)) {
            normalized <<= 1;
        }
        return normalized;
    }

    Shard& GetShard(std::string_view key) {
        return shards_[std::hash<std::string_view>{}(key) & shard_mask_];
    }

    const size_t capacity_;
    std::vector<Shard> shards_;
    const size_t shard_mask_;
    std::atomic<size_t> size_{0};
};

}  // namespace mooncake
