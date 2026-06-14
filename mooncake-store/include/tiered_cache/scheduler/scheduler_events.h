#pragma once

#include <cstddef>
#include <optional>
#include <string_view>

#include "types.h"

namespace mooncake {

/**
 * @enum AccessOrigin
 * @brief Distinguishes whether an access event came from a read (Get) or a
 *        write (Commit). Only Get accesses participate in frequency counting
 *        and offload/onboard routing.
 */
enum class AccessOrigin { kGet, kCommit };

/**
 * @struct AccessContext
 * @brief Context for a single access event reported to the scheduler.
 *
 * `served_tier_id` / `served_tier_type` describe which tier actually served a
 * Get; they are only meaningful when `origin == kGet`.
 */
struct AccessContext {
    std::string_view key;
    AccessOrigin origin = AccessOrigin::kGet;
    std::optional<UUID> served_tier_id;
    MemoryType served_tier_type = MemoryType::UNKNOWN;
};

/**
 * @struct CommitContext
 * @brief Context for a committed (newly written or updated) replica.
 */
struct CommitContext {
    std::string_view key;
    UUID tier_id;
    size_t size_bytes = 0;
};

/**
 * @struct DeleteContext
 * @brief Context for a replica or key deletion. `tier_id == nullopt` means the
 *        whole key (all replicas) was removed.
 */
struct DeleteContext {
    std::string_view key;
    std::optional<UUID> tier_id;
};

/**
 * @struct AllocationFailureContext
 * @brief Context for an allocation failure on a specific tier.
 */
struct AllocationFailureContext {
    UUID tier_id;
    size_t required_bytes = 0;
};

}  // namespace mooncake
