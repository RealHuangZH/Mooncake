#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "tiered_cache/scheduler/bounded_dedup_queue.h"

namespace mooncake {
namespace {

TEST(BoundedDedupQueueTest, DedupRejectsKeyAlreadyInFlight) {
    BoundedDedupQueue queue(8);
    EXPECT_TRUE(queue.TryAdmit("a"));
    EXPECT_FALSE(queue.TryAdmit("a")) << "a is already in flight";
    EXPECT_EQ(queue.InFlightCount(), 1u);

    queue.MarkDone("a");
    EXPECT_EQ(queue.InFlightCount(), 0u);
    EXPECT_TRUE(queue.TryAdmit("a")) << "re-admittable after MarkDone";
}

TEST(BoundedDedupQueueTest, BoundedCapacityDropsWhenFull) {
    BoundedDedupQueue queue(/*capacity=*/3);
    EXPECT_TRUE(queue.TryAdmit("a"));
    EXPECT_TRUE(queue.TryAdmit("b"));
    EXPECT_TRUE(queue.TryAdmit("c"));
    EXPECT_EQ(queue.InFlightCount(), 3u);
    EXPECT_FALSE(queue.TryAdmit("d")) << "queue is full";

    queue.MarkDone("b");
    EXPECT_TRUE(queue.TryAdmit("d")) << "slot freed for d";
    EXPECT_EQ(queue.InFlightCount(), 3u);
}

TEST(BoundedDedupQueueTest, MarkDoneUnknownKeyIsNoOp) {
    BoundedDedupQueue queue(4);
    EXPECT_TRUE(queue.TryAdmit("x"));
    queue.MarkDone("never_admitted");
    EXPECT_EQ(queue.InFlightCount(), 1u);
}

TEST(BoundedDedupQueueTest, ConcurrentChurnNeverUnderflowsOrExceeds) {
    constexpr size_t kCapacity = 64;
    BoundedDedupQueue queue(kCapacity, /*shard_count=*/8);
    std::atomic<int> exceeded{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < 8; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < 20000; ++i) {
                std::string key = "k" + std::to_string((t * 7 + i) % 200);
                if (queue.TryAdmit(key)) {
                    if (queue.InFlightCount() > kCapacity) exceeded++;
                    queue.MarkDone(key);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();

    EXPECT_EQ(queue.InFlightCount(), 0u) << "all slots released";
    EXPECT_EQ(exceeded.load(), 0) << "capacity never exceeded";
}

}  // namespace
}  // namespace mooncake
