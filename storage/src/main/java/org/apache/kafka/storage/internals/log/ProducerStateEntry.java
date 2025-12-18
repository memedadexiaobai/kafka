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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * This class represents the state of a specific producer-id.
 * It contains batchMetadata queue which is ordered such that
 *  the batch with the lowest sequence is at the head of the queue
 *  while the batch with the highest sequence is at the tail of the queue.
 * We will retain(保留) at most(至多) {@link ProducerStateEntry#NUM_BATCHES_TO_RETAIN} elements in the queue.
 * When the queue is at capacity, we remove the first element to make space for the incoming batch.
 */
public class ProducerStateEntry {
    public static final int NUM_BATCHES_TO_RETAIN = 5;
    private final long producerId;
    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>();

    private short producerEpoch;
    private int coordinatorEpoch;
    private long lastTimestamp;
    /**
     * 专门用来判断“这个生产者当前有没有尚未提交的事务”，并记录该事务的第一条消息的偏移量。
     *  Some(firstOffset) → 该生产者 正处于一个打开的事务中，事务的第一条消息位于 firstOffset。
     *  None → 该生产者 目前没有活跃事务（要么已提交/中止，要么根本没开事务）
     *
     *  | 场景                    | 判断逻辑                                                                                                       | 作用                          |
     * | ------------------      | -----------------------------------------------------------------------------------                           | --------------------------- |
     * | **事务提交/中止**         | 如果 `currentTxnFirstOffset.isDefined`，说明还有事务未结束，需要生成 **事务结束标记**（commit/abort marker）         | 决定是否写 `.txnindex` 文件、更新 LSO |
     * | **事务超时**             | Coordinator 扫描到 `currentTxnFirstOffset` 存在且超时 → 触发 **强制 abort**                                      | 保证挂起事务不会永久占用资源              |
     * | **Consumer 读事务消息**  | 只读 **已提交** 数据；遇到 `currentTxnFirstOffset` 到 **事务结束标记** 之间的消息，若事务最终 abort，则跳过这批消息     | 实现 **read-committed** 隔离级别  |
     * | **快照/日志恢复**        | 快照里保存 `currentTxnFirstOffset`；Broker 重启后恢复生产者状态，继续跟踪未结束的事务                                | 保证事务状态不丢                    |
     *
     * producerId: 3000
     * producerEpoch: 5
     * currentTxnFirstOffset: 235947651   // 事务开始位置
     *  生产者 3000 从 offset 235947651 开始了一个事务，还没提交或中止，后续所有消息都属于该事务，直到看到对应的 COMMIT/ABORT 标记 才算结束
     *
     * currentTxnFirstOffset 就是 “事务开关”：
     * 有值 → 事务开着；没值 → 事务关着——Kafka 靠它决定 要不要收尾、要不要过滤、要不要恢复。
     */
    private OptionalLong currentTxnFirstOffset;

    public static ProducerStateEntry empty(long producerId) {
        return new ProducerStateEntry(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty());
    }

    public ProducerStateEntry(long producerId, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset, Optional<BatchMetadata> firstBatchMetadata) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        firstBatchMetadata.ifPresent(batchMetadata::add);
    }

    public int firstSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getFirst().firstSeq();
    }

    public int lastSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getLast().lastSeq;
    }

    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getFirst().firstOffset();
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getLast().lastOffset;
    }

    public int lastOffsetDelta() {
        return isEmpty() ? 0 : batchMetadata.getLast().offsetDelta;
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    /**
     * Returns a new instance with the provided parameters (when present) and the values from the current instance
     * otherwise.
     */
    public ProducerStateEntry withProducerIdAndBatchMetadata(long producerId, Optional<BatchMetadata> batchMetadata) {
        return new ProducerStateEntry(producerId, this.producerEpoch(), this.coordinatorEpoch, this.lastTimestamp,
            this.currentTxnFirstOffset, batchMetadata);
    }

    public void addBatch(short producerEpoch, int lastSeq, long lastOffset, int offsetDelta, long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    /**
     * producerEpoch不比较大小，只要“不相等”就立即清空缓存并采纳新 epoch——这是 Kafka 幂等/事务语义 的硬性规定：
     *  epoch 是“会话世代”号，只能单调递增，但“谁大谁小”由 TransactionCoordinator 保证；Broker 侧只做“换代”动作，不做数值校验。
     *
     * epoch 的本质
     *  producer id 不变，epoch 每次 producer 重启 +1（由 TransactionCoordinator 分配并持久化到 __transaction_state）。
     *  同一 PID 旧 epoch 的任何重试批都会被拒绝（sequence 必须连续，但 epoch 已变 → 直接抛 OutOfOrderSequenceException）。
     *  因此 Broker 只要发现 epoch 变了，就代表“旧会话已死”，内存里的旧批次缓存立即失效。
     * 为什么“不清空”反而危险
     *  若保留旧 epoch 的 batchMetadata，新会话重试时可能会误命中“自己序号段”，导致 去重逻辑错误。
     *  清空后，新会话从 seq=0 开始重攒缓存，与快照中记录的“新 epoch 的 lastSequence”严格衔接，保证序列号断层。
     * 数值大小谁保证
     *  TransactionCoordinator 在分配新 epoch 时已做 +1 并持久化；
     *  Broker 从 FetchResponse 或 ProduceRequest 里拿到的 epoch 必然 ≥ 旧值，无需再比较
     *
     */
    public boolean maybeUpdateProducerEpoch(short producerEpoch) {
        if (this.producerEpoch != producerEpoch) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    /**
     * BatchMetadata 只含 3 个字段：
     *  lastSeq　　// 本批最后一条序列号
     *  lastOffset　// 本批最后一条分区偏移
     *  timestamp　// 批时间戳
     * 目的：
     *  当客户端 重试发送同一批（网络闪断）时，Broker 可以 O(1) 在内存里发现“这组 Seq 已存在”，直接返回成功而不再写磁盘，实现 幂等去重。
     *
     * 为什么只保留 NUM_BATCHES_TO_RETAIN（默认 5）
     *  重试窗口有限：Kafka 生产者 默认 retries < Integer.MAX_VALUE 且 in-flight ≤ 5；
     *  最多同时 5 批在空中，再早的批早已收到响应或超时，不会重试。
     *  内存友好：每分区只留 5 份 24 字节 ≈ 120 B，百万分区也只 120 MB。
     *  快照已备份：Broker 每 segment roll 都会打 .snapshot，里面已含 所有 PID 的 lastSeq 和 lastOffset；
     *  即使缓存淘汰，重启后仍能从 快照 + 日志重放 恢复 lastSequence，不会破坏幂等语义
     *
     * 淘汰最旧头节点会“丢”吗
     *  | 场景                | 是否受影响                                                                  |
     *  | -------------      | ---------------------------------------------------------------------- |
     *  | **正常重试**        | **不会**——重试批一定在 **最近 5 批**范围内。                                          |
     *  | **异常超大重试**    | **也不会**——**lastSequence 快照值**已持久化；**重试批 Seq 必须 ≥ 快照值 +1**，**否则仍会被拒绝**。 |
     *  | **Broker 重启**   | **不会**——重启后 **先加载快照** → **lastSeq 已恢复** → **缓存重新从 0 开始攒**。             |
     *
     * “5 长度循环队列”只是 热缓存去重窗口，不是可靠性存储；
     *    真正序列号锚点已落盘到快照，淘汰早期缓存只影响“极旧重试”性能，不影响幂等正确性，因此不会丢数据
     */
    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NUM_BATCHES_TO_RETAIN) batchMetadata.removeFirst();
        batchMetadata.add(batch);
    }

    public void update(ProducerStateEntry nextEntry) {
        update(nextEntry.producerEpoch, nextEntry.coordinatorEpoch, nextEntry.lastTimestamp, nextEntry.batchMetadata, nextEntry.currentTxnFirstOffset);
    }

    public void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp) {
        update(producerEpoch, coordinatorEpoch, lastTimestamp, new ArrayDeque<>(0), OptionalLong.empty());
    }

    private void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp, Deque<BatchMetadata> batchMetadata,
                        OptionalLong currentTxnFirstOffset) {
        maybeUpdateProducerEpoch(producerEpoch);
        while (!batchMetadata.isEmpty())
            addBatchMetadata(batchMetadata.removeFirst());
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        this.lastTimestamp = lastTimestamp;
    }

    public void setCurrentTxnFirstOffset(long firstOffset) {
        this.currentTxnFirstOffset = OptionalLong.of(firstOffset);
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        if (batch.producerEpoch() != producerEpoch) return Optional.empty();
        else return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
    }

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    Optional<BatchMetadata> batchWithSequenceRange(int firstSeq, int lastSeq) {
        Stream<BatchMetadata> duplicate = batchMetadata.stream().filter(metadata -> firstSeq == metadata.firstSeq() && lastSeq == metadata.lastSeq);
        return duplicate.findFirst();
    }

    public Collection<BatchMetadata> batchMetadata() {
        return Collections.unmodifiableCollection(batchMetadata);
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public long producerId() {
        return producerId;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public OptionalLong currentTxnFirstOffset() {
        return currentTxnFirstOffset;
    }

    @Override
    public String toString() {
        return "ProducerStateEntry(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", currentTxnFirstOffset=" + currentTxnFirstOffset +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", lastTimestamp=" + lastTimestamp +
                ", batchMetadata=" + batchMetadata +
                ')';
    }
}
