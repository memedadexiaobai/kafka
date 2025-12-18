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

import org.apache.kafka.common.KafkaException;

/**
 * A log offset structure, including:
 *  1. the message offset
 *  2. the base message offset of the located segment
 *  3. the physical position on the located segment
 *
 *  Kafka 里 “一条逻辑偏移到底长什么样” 的微型快照——3 个 long 就能让 Broker 在任何时候、任何文件、任何位置都能“秒级”重新定位，并且 区分“逻辑位”与“物理位”。
 *  | 字段                  | 通俗解释                                            | 取值示例                       |
 *  | -------------------  | ----------------------------------------           | -------------------------- |
 *  | `messageOffset`     | **分区视角的序号**<br>消费者、副本都认它                 | `topic-partition=5`        |
 *  | `segmentBaseOffset` | **段文件名**<br>用来快速定位到哪个 `.log` 文件          | `00000000000000000005.log` |
 *  | `relativePosition`  | **段内字节偏移**<br>告诉 `FileChannel` **从哪开始读**   | `1024`（第 1 KB 处）           |
 *  “我要读分区 offset=5 的消息” → 先找到文件 5.log，再 seek 到 1024 字节，后面就是消息本体。
 *
 * | 维度               | 逻辑（Logical）                                            | 物理（Physical）                       |
 * | -------------     | --------------------------------------------------        | ---------------------------------- |
 * | **名称**          | `messageOffset`（分区级序号）                                | `relativePosition`（文件内字节号）         |
 * | **全局唯一**       | ✅ 整个分区单调递增                                         | ❌ 只在单个段文件里有效                       |
 * | **消费者可见**     | ✅ 订阅/消费进度用它                                        | ❌ 对外完全透明                           |
 * | **Broker 内部**   | 先转 **segmentBaseOffset** → 再转 **relativePosition**     | 最终 `FileChannel.read(position)` 用它 |
 * | **丢失/损坏**     | 段文件被截断后，**逻辑 offset 连续**，物理位置向后跳            | 物理位置可能 **不连续**（索引稀疏、batch 大小不一）    |
 *
 * 类比图书馆
 * 逻辑 offset = 书的页码（读者只认页码）
 * 物理 position = 打开文件第多少字节（管理员定位用）
 * segmentBaseOffset = 哪一卷/哪一册（文件名）
 *
 */
public final class LogOffsetMetadata {

    //TODO KAFKA-14484 remove once UnifiedLog has been moved to the storage module
    private static final long UNIFIED_LOG_UNKNOWN_OFFSET = -1L;
    private static final int UNKNOWN_FILE_POSITION = -1;

    public static final LogOffsetMetadata UNKNOWN_OFFSET_METADATA = new LogOffsetMetadata(-1L, UNIFIED_LOG_UNKNOWN_OFFSET, UNKNOWN_FILE_POSITION);

    public final long messageOffset;  // ① 逻辑偏移量（分区级）
    public final long segmentBaseOffset;  // ② 所在段的基偏移
    public final int relativePositionInSegment;  // ③ 在该段文件里的**物理字节位置**

    public LogOffsetMetadata(long messageOffset) {
        this(messageOffset, UNIFIED_LOG_UNKNOWN_OFFSET, UNKNOWN_FILE_POSITION);
    }

    public LogOffsetMetadata(long messageOffset,
                             long segmentBaseOffset,
                             int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // check if this offset is already on an older segment compared with the given offset
    public boolean onOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly() || that.messageOffsetOnly())
            return false;
        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    // check if this offset is on the same segment with the given offset
    public boolean onSameSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly() || that.messageOffsetOnly())
            return false;
        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public int positionDiff(LogOffsetMetadata that) {
        if (messageOffsetOnly() || that.messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since it only has message offset info");
        if (!onSameSegment(that))
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since they are not on the same segment");

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    // decide if the offset metadata only contains message offset info
    public boolean messageOffsetOnly() {
        return segmentBaseOffset == UNIFIED_LOG_UNKNOWN_OFFSET && relativePositionInSegment == UNKNOWN_FILE_POSITION;
    }

    @Override
    public String toString() {
        return "(offset=" + messageOffset + ", segment=[" + segmentBaseOffset + ":" + relativePositionInSegment + "])";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogOffsetMetadata that = (LogOffsetMetadata) o;
        return messageOffset == that.messageOffset
                && segmentBaseOffset == that.segmentBaseOffset
                && relativePositionInSegment == that.relativePositionInSegment;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(messageOffset);
        result = 31 * result + Long.hashCode(segmentBaseOffset);
        result = 31 * result + Integer.hashCode(relativePositionInSegment);
        return result;
    }
}
