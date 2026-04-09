/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Built-in default partitioner.  Note, that this is just a utility class that is used directly from
 * RecordAccumulator, it does not implement the Partitioner interface.
 *
 * The class keeps track of various bookkeeping information required for adaptive sticky partitioning
 * (described in detail in KIP-794).  There is one partitioner object per topic.
 */
public class BuiltInPartitioner {
    private final Logger log;
    private final String topic;
    private final int stickyBatchSize;

    private volatile PartitionLoadStats partitionLoadStats = null; // 分区负载统计信息，用于动态调整分区选择策略。
    private final AtomicReference<StickyPartitionInfo> stickyPartitionInfo = new AtomicReference<>();


    /**
     * BuiltInPartitioner constructor.
     *
     * @param topic The topic
     * @param stickyBatchSize How much to produce to partition before switch
     */
    public BuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize) {
        this.log = logContext.logger(BuiltInPartitioner.class);
        this.topic = topic;
        if (stickyBatchSize < 1) {
            throw new IllegalArgumentException("stickyBatchSize must be >= 1 but got " + stickyBatchSize);
        }
        this.stickyBatchSize = stickyBatchSize;
    }

    /**
     * Calculate the next partition for the topic based on the partition load stats.
     */
    private int nextPartition(Cluster cluster) {
        int random = randomPartition(); // 用于均匀分布或加权随机选择。

        // Cache volatile variable in local variable.
        PartitionLoadStats partitionLoadStats = this.partitionLoadStats;
        int partition;

        if (partitionLoadStats == null) {
            // We don't have stats to do adaptive partitioning (or it's disabled), just switch to the next
            // partition based on uniform distribution(词组：均匀分布).
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (!availablePartitions.isEmpty()) {
                partition = availablePartitions.get(random % availablePartitions.size()).partition();
            } else {
                // We don't have available partitions, just pick one among all partitions.
                List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
                partition = random % partitions.size();//本身就都不可用，求余的结果也是分区id 直接返回就行，没必要再进一步了
            }
        } else {
            // Calculate next partition based on load distribution(分布). 负载分布
            // Note that partitions without leader are excluded from the partitionLoadStats.
            // 通过累积频率表和加权随机选择，实现了基于分区负载的动态分区选择逻辑。
            assert partitionLoadStats.length > 0;

            /**
             * 作用：获取分区负载统计的累积频率表。
             * 细节：cumulativeFrequencyTable 是一个整型数组，存储了按分区负载排序后的累积频率值。这些值用于表示每个分区的相对负载，后续会根据这些频率进行加权随机选择。
             */
            int[] cumulativeFrequencyTable = partitionLoadStats.cumulativeFrequencyTable;
            /**
             * 作用：根据累积频率的总和计算一个加权随机值。
             * 细节：
             *  random 是一个随机数，用于引入随机性。
             *  cumulativeFrequencyTable[partitionLoadStats.length - 1] 是累积频率表的最后一个元素，表示所有分区负载的总和。
             *  weightedRandom 是对总和取模后的结果，用于确保随机值落在累积频率的范围内。这个值将用于在累积频率表中查找对应的分区。
             */
            int weightedRandom = random % cumulativeFrequencyTable[partitionLoadStats.length - 1];

            /**
             * By construction(构造), the cumulative frequency table is sorted, so we can use binary search to find the desired index.
             * 作用：在累积频率表中查找 weightedRandom 值的位置。
             * 细节：
             *   使用二分查找算法在 cumulativeFrequencyTable 的指定范围内查找 weightedRandom。
             *   如果找到确切的匹配项，searchResult 是该值的索引。
             *   如果没有找到确切的匹配项，返回值为 -(insertion point) - 1，其中 insertion point 是该值应插入的位置，以保持数组的有序性。
             */
            int searchResult = Arrays.binarySearch(cumulativeFrequencyTable, 0, partitionLoadStats.length, weightedRandom);

            /**
             * binarySearch results the index of the found element, or -(insertion_point) - 1
             * (where insertion_point is the index of the first element greater than the key).
             * We need to get the index of the first value that is strictly(严格) greater, which
             * would be the insertion point, except if we found the element that's equal to
             * the searched value (in this case we need to get next).  For example, if we have
             *  4 5 8
             * and we're looking for 3, then we'd get the insertion_point = 0, and the function
             * would return -0 - 1 = -1, by adding 1 we'd get 0.  If we're looking for 4, we'd
             *  get 0, and we need the next one, so adding 1 works here as well.
             *
             * 作用：根据查找结果计算分区索引。
             * 细节：
             *  searchResult + 1 是为了调整二分查找的返回值，以获取正确的分区索引。
             *  Math.abs 确保结果为非负数，避免数组索引出现负值。
             *  partitionIndex 是最终确定的分区索引，用于选择目标分区。
             */
            int partitionIndex = Math.abs(searchResult + 1);
            assert partitionIndex < partitionLoadStats.length;
            partition = partitionLoadStats.partitionIds[partitionIndex];
        }

        log.trace("Switching to partition {} in topic {}", partition, topic);
        return partition;
    }

    /**
     * 为什么使用 ThreadLocalRandom 而不是 java.util.Random？
     *   核心原因：高并发场景下的性能优化
     * // ❌ 方案 1: 使用 java.util.Random (不推荐)
     * public class BadExample {
     *     private static final Random random = new Random(); // 共享实例
     *
     *     public int getNextPartition() {
     *         // 问题：多线程并发访问同一个 Random 实例时，
     *         // seed 的更新需要使用 CAS (Compare-And-Swap) 操作
     *         // 在高并发下会产生严重的锁竞争
     *         return random.nextInt();
     *     }
     * }
     *
     * // ✅ 方案 2: 使用 ThreadLocalRandom (Kafka 的选择)
     * public class GoodExample {
     *     public int getNextPartition() {
     *         // 每个线程有自己的 Random 实例，无锁竞争
     *         return ThreadLocalRandom.current().nextInt();
     *     }
     * }
     * | 特性 | `java.util.Random` | `ThreadLocalRandom` |
     * |------|-------------------|---------------------|
     * | **线程安全性** | 使用 CAS 更新种子 (原子操作) | 每个线程独立实例，无需同步 |
     * | **并发性能** | 高并发下 CAS 竞争激烈，性能下降 | 无锁，性能稳定 |
     * | **内存占用** | 单个实例 | 每个线程一个实例 |
     * | **适用场景** | 低并发、单线程 | 高并发、多线程 |
     * 在 100 线程高并发场景下，ThreadLocalRandom 的性能通常是 Random 的 3-10 倍。
     *
     * Kafka 选择 ThreadLocalRandom 的原因：
     *  ✅ 无锁设计：避免多线程 CAS 竞争
     *  ✅ 高吞吐：适合生产者高并发场景
     *  ✅ 确定性：配合 Utils.toPositive() 确保分区算法的稳定性
     *  ✅ JDK 推荐：Java 7+ 官方推荐的并发随机数生成方式
     * 正如代码注释所说，这个分区逻辑会影响 sticky batch 的行为，进而影响批处理效率和网络传输性能，
     * 所以选择合适的随机数生成器对 Kafka 的整体性能至关重要。
     */
    int randomPartition() {
        return Utils.toPositive(ThreadLocalRandom.current().nextInt());
    }

    /**
     * Test-only function.  When partition load stats are defined, return the end of range for the
     * random number.
     */
    public int loadStatsRangeEnd() {
        assert partitionLoadStats != null;
        assert partitionLoadStats.length > 0;
        return partitionLoadStats.cumulativeFrequencyTable[partitionLoadStats.length - 1];
    }

    /**
     * Peek currently chosen sticky(黏性的) partition.  This method works in conjunction with(与...一起) {@link #isPartitionChanged}
     * and {@link #updatePartitionInfo}.  The workflow is the following:
     *
     * 1. peekCurrentPartitionInfo is called to know which partition to lock.
     * 2. Lock partition's batch queue.
     * 3. isPartitionChanged under lock to make sure that nobody raced us.
     * 4. Append data to buffer.
     * 5. updatePartitionInfo to update produced bytes and maybe switch partition.
     *
     *  It's important that steps 3-5 are under partition's batch queue lock.
     *
     * @param cluster The cluster information (needed if there is no current partition)
     * @return sticky partition info object
     */
    StickyPartitionInfo peekCurrentPartitionInfo(Cluster cluster) {
        //优先 stickyPartitionInfo
        StickyPartitionInfo partitionInfo = stickyPartitionInfo.get();
        if (partitionInfo != null)
            return partitionInfo;

        // We're the first to create it. Sticky：黏性
        partitionInfo = new StickyPartitionInfo(nextPartition(cluster));
        if (stickyPartitionInfo.compareAndSet(null, partitionInfo))
            return partitionInfo;

        // Someone has raced us. 有别的线程先CAS了 直接获取
        return stickyPartitionInfo.get();
    }

    /**
     * Check if partition is changed by a concurrent thread.  NOTE this function needs to be called under
     * the partition's batch queue lock.
     *
     * @param partitionInfo The sticky partition info object returned by peekCurrentPartitionInfo
     * @return true if sticky partition object is changed (race condition)
     */
    boolean isPartitionChanged(StickyPartitionInfo partitionInfo) {
        // partitionInfo may be null if the caller didn't use built-in partitioner.
        return partitionInfo != null && stickyPartitionInfo.get() != partitionInfo;
    }

    /**
     * Update partition info with the number of bytes appended and maybe switch partition.
     * NOTE this function needs to be called under the partition's batch queue lock.
     *
     * @param partitionInfo The sticky partition info object returned by peekCurrentPartitionInfo
     * @param appendedBytes The number of bytes appended to this partition
     * @param cluster The cluster information
     */
    void updatePartitionInfo(StickyPartitionInfo partitionInfo, int appendedBytes, Cluster cluster) {
        updatePartitionInfo(partitionInfo, appendedBytes, cluster, true);
    }

    /**
     * Update partition info with the number of bytes appended and maybe switch partition.
     * NOTE this function needs to be called under the partition's batch queue lock.
     *
     * @param partitionInfo The sticky partition info object returned by peekCurrentPartitionInfo
     * @param appendedBytes The number of bytes appended to this partition
     * @param cluster The cluster information
     * @param enableSwitch If true, switch partition once produced enough bytes 队列满了
     */
    void updatePartitionInfo(StickyPartitionInfo partitionInfo, int appendedBytes, Cluster cluster, boolean enableSwitch) {
        // partitionInfo may be null if the caller didn't use built-in partitioner.
        if (partitionInfo == null)
            return;

        assert partitionInfo == stickyPartitionInfo.get();
        int producedBytes = partitionInfo.producedBytes.addAndGet(appendedBytes);

        // We're trying to switch partition once we produce stickyBatchSize bytes to a partition
        // but doing so may hinder batching because partition switch may happen while batch isn't
        // ready to send.  This situation is especially likely with high linger.ms setting.
        // Consider the following example:
        //   linger.ms=500, producer produces 12KB in 500ms, batch.size=16KB
        //     - first batch collects 12KB in 500ms, gets sent
        //     - second batch collects 4KB, then we switch partition, so 4KB gets eventually sent
        //     - ... and so on - we'd get 12KB and 4KB batches
        // To get more optimal batching and avoid 4KB fractional batches, the caller may disallow
        // partition switch if batch is not ready to send, so with the example above we'd avoid
        // fractional 4KB batches: in that case the scenario would look like this:
        //     - first batch collects 12KB in 500ms, gets sent
        //     - second batch collects 4KB, but partition switch doesn't happen because batch in not ready
        //     - second batch collects 12KB in 500ms, gets sent and now we switch partition.
        //     - ... and so on - we'd just send 12KB batches
        // We cap the produced bytes to not exceed 2x of the batch size to avoid pathological cases
        // (e.g. if we have a mix of keyed and unkeyed messages, key messages may create an
        // unready batch after the batch that disabled partition switch becomes ready).
        // As a result, with high latency.ms setting we end up switching partitions after producing
        // between stickyBatchSize and stickyBatchSize * 2 bytes, to better align with batch boundary.
        if (producedBytes >= stickyBatchSize * 2) {
            log.trace("Produced {} bytes, exceeding twice the batch size of {} bytes, with switching set to {}",
                producedBytes, stickyBatchSize, enableSwitch);
        }

        // 当这个分区处理的字节数producedBytes大于了stickyBatchSize同时允许切换分区或者producedBytes大于了stickyBatchSize * 2 就切下个分区
        if (producedBytes >= stickyBatchSize && enableSwitch || producedBytes >= stickyBatchSize * 2) {
            // We've produced enough to this partition, switch to next.
            StickyPartitionInfo newPartitionInfo = new StickyPartitionInfo(nextPartition(cluster));
            stickyPartitionInfo.set(newPartitionInfo);
        }
    }

    /**
     * Update partition load stats from the queue sizes of each partition
     * NOTE: queueSizes are modified in place to avoid allocations
     *
     * 为什么设计累积频率表 + 二分查找的自适应分区算法？
     *  核心设计目标：根据分区负载动态调整选择概率，实现负载均衡
     *
     * 假设我们有 3 个分区，它们的队列长度（待发送消息数）
     *  分区 0: 队列长度 = 0  (空闲)
     *  分区 1: 队列长度 = 3  (繁忙)
     *  分区 2: 队列长度 = 1  (一般)
     * 步骤 1️⃣：反转队列长度 → 转换为"权重"
     *  int maxSizePlus1 = max(0, 3, 1) + 1 = 4;
     *
     *  // 反转：用最大值减去当前值
     *  分区 0 权重 = 4 - 0 = 4  (队列越空，权重越高) ✅
     *  分区 1 权重 = 4 - 3 = 1  (队列越忙，权重越低) ❌
     *  分区 2 权重 = 4 - 1 = 3  (中等权重)
     * 为什么要反转？
     *  队列长度 越小（空闲）→ 希望被选中的概率 越大
     *  队列长度 越大（繁忙）→ 希望被选中的概率 越小
     * 步骤 2️⃣：构建累积频率表
     *  原始权重：[4, 1, 3]
     *  累积求和：
     *  cumulativeFrequencyTable[0] = 4                    = 4
     *  cumulativeFrequencyTable[1] = 4 + 1                = 5
     *  cumulativeFrequencyTable[2] = 4 + 1 + 3            = 8
     *  结果：[4, 5, 8]
     *        ↑  ↑  ↑
     *        |  |  └─ 分区 2 的范围：[5, 6, 7] (3 个数)
     *        |  └──── 分区 1 的范围：[4]   (1 个数)
     *        └─────── 分区 0 的范围：[0-3] (4 个数)
     * 随机数范围 [0..8):
     * 0  1  2  3| 4 |5 6  7
     * ├─────────┼───┼──────────┤
     * │ 分区 0   │ 1 │  分区 2   │
     * │ (4 个数) │   │ (3 个数)  │
     * └─────────┴───┴──────────┘
     *      4           3
     *
     * 🔍 为什么使用这种设计？
     *  1️⃣ O(log n) 的时间复杂度：线性扫描 O(n) vs 二分查找 O(log n)
     *  2️⃣ 空间效率优化：不需要额外数组，直接在原数组上操作 减少内存分配和 GC 压力
     *  3️⃣ 处理边界情况：只有 0 或 1 个分区或所有队列长度相同 不需要自适应
     *  4️⃣ 与 Sticky Partition 配合
     *   粘性分区：一段时间内固定发送到同一个分区
     *   好处：提高批处理效率，减少网络传输
     *   切换时机：当生产的数据量达到 stickyBatchSize 时，才重新调用 nextPartition() 选择新分区
     *   自适应：每次切换都基于最新的负载统计，动态调整
     *| 分区 | 队列长度 | 传统轮询 | 自适应算法 |
     * |------|---------|---------|-----------|
     * | 分区 0 | 0 | 33.3% | **50%** ✅ |
     * | 分区 1 | 3 | 33.3% | **12.5%** ✅ |
     * | 分区 2 | 1 | 33.3% | **37.5%** ✅ |
     * 传统轮询：不管负载，平均分配
     * 自适应算法：向空闲分区倾斜，降低繁忙分区的压力
     *
     * 🎯设计的精妙之处
     *  数学优雅：将"队列长度"反转为"权重"，自然实现"负载越低，概率越高"
     *  性能优秀：O(log n) 二分查找，适合高频调用场景
     *  空间高效：就地转换，零额外内存分配
     *  动态适应：实时响应分区负载变化
     *  兼容性好：与 Sticky Partition 机制无缝集成
     *  退化安全：特殊情况自动降级到简单算法
     * 这就是 KIP-794 提出的自适应粘性分区器的核心算法，它显著提升了 Kafka 生产者在不均匀负载场景下的性能表现！
     *
     * @param queueSizes The queue sizes, partitions without leaders are excluded
     * @param partitionIds The partition ids for the queues, partitions without leaders are excluded
     * @param length The logical length of the arrays (could be less): we may eliminate(消除) some partitions
     *               based on latency(延迟), but to avoid reallocation of the arrays, we just decrement
     *               logical length
     * Visible for testing
     */
    public void updatePartitionLoadStats(int[] queueSizes, int[] partitionIds, int length) {
        if (queueSizes == null) {
            log.trace("No load stats for topic {}, not using adaptive", topic);
            partitionLoadStats = null;
            return;
        }
        assert queueSizes.length == partitionIds.length;
        assert length <= queueSizes.length;

        // The queueSizes.length represents the number of all partitions in the topic and if we have
        // less than 2 partitions, there is no need to do adaptive logic.
        // If partitioner.availability.timeout.ms != 0, then partitions that experience high latencies
        // (greater than partitioner.availability.timeout.ms) may be excluded, the length represents
        // partitions that are not excluded.  If some partitions were excluded, we'd still want to
        // go through adaptive logic, even if we have one partition.
        // See also RecordAccumulator#partitionReady where the queueSizes are built.
        if (length < 1 || queueSizes.length < 2) { // 只有 0 或 1 个分区，不需要自适应
            log.trace("The number of partitions is too small: available={}, all={}, not using adaptive for topic {}",
                    length, queueSizes.length, topic);
            partitionLoadStats = null;
            return;
        }

        // We build cumulative(累积) frequency table from the queue sizes in place(就地).  At the beginning
        // each entry contains queue size, then we invert it (so it represents the frequency)
        // and convert to a running sum.  Then a uniformly distributed random variable
        // in the range [0..last) would map to a partition with weighted probability.
        // Example: suppose we have 3 partitions with the corresponding queue sizes:
        //  0 3 1
        // Then we can invert(反转) them by subtracting the queue size from the max queue size + 1 = 4:
        //  4 1 3
        // Then we can convert it into a running sum (next value adds previous value):
        //  4 5 8
        // Now if we get a random number in the range [0..8) and find the first value that
        // is strictly greater than the number (e.g. for 4 it would be 5), then the index of
        // the value is the index of the partition we're looking for.  In this example
        // random numbers 0, 1, 2, 3 would map to partition[0], 4 would map to partition[1]
        // and 5, 6, 7 would map to partition[2].

        // Calculate max queue size + 1 and check if all sizes are the same.
        int maxSizePlus1 = queueSizes[0];
        boolean allEqual = true;
        for (int i = 1; i < length; i++) {
            if (queueSizes[i] != maxSizePlus1)
                allEqual = false;
            if (queueSizes[i] > maxSizePlus1)
                maxSizePlus1 = queueSizes[i];
        }
        ++maxSizePlus1;//跳出最大的maxSizePlus1 再+1

        if (allEqual && length == queueSizes.length) {//所有队列长度相同，不需要自适应
            // No need to have complex probability logic when all queue sizes are the same,
            // and we didn't exclude partitions that experience high latencies (greater than
            // partitioner.availability.timeout.ms).
            log.trace("All queue lengths are the same, not using adaptive for topic {}", topic);
            partitionLoadStats = null;
            return;
        }

        // Invert and fold the queue size, so that they become separator values in the CFT.
        queueSizes[0] = maxSizePlus1 - queueSizes[0];
        for (int i = 1; i < length; i++) {
            queueSizes[i] = maxSizePlus1 - queueSizes[i] + queueSizes[i - 1];
        }
        log.trace("Partition load stats for topic {}: CFT={}, IDs={}, length={}",
                topic, queueSizes, partitionIds, length);
        partitionLoadStats = new PartitionLoadStats(queueSizes, partitionIds, length);
    }

    /**
     * Info for the current sticky partition.
     */
    public static class StickyPartitionInfo {
        private final int index;
        private final AtomicInteger producedBytes = new AtomicInteger();

        StickyPartitionInfo(int index) {
            this.index = index;
        }

        public int partition() {
            return index;
        }
    }

    /*
     * Default hashing function to choose a partition from the serialized key bytes
     */
    public static int partitionForKey(final byte[] serializedKey, final int numPartitions) {
        return Utils.toPositive(Utils.murmur2(serializedKey)) % numPartitions;
    }

    /**
     * The partition load stats for each topic that are used for adaptive(自适应的) partition distribution.
     */
    private static final class PartitionLoadStats {
        public final int[] cumulativeFrequencyTable; // 累积频数表
        public final int[] partitionIds;
        public final int length;

        public PartitionLoadStats(int[] cumulativeFrequencyTable, int[] partitionIds, int length) {
            assert cumulativeFrequencyTable.length == partitionIds.length;
            assert length <= cumulativeFrequencyTable.length;
            this.cumulativeFrequencyTable = cumulativeFrequencyTable;
            this.partitionIds = partitionIds;
            this.length = length;
        }
    }
}
