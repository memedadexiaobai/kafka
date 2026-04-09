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
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

public abstract class AbstractRecords implements Records {

    private final Iterable<Record> records = this::recordsIterator;

    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    public RecordBatch firstBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        if (!iterator.hasNext())
            return null;

        return iterator.next();
    }

    @Override
    public Optional<RecordBatch> lastBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        RecordBatch batch = null;
        while (iterator.hasNext())
            batch = iterator.next();

        return Optional.ofNullable(batch);
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    @Override
    public DefaultRecordsSend<Records> toSend() {
        return new DefaultRecordsSend<>(this);
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<Record> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    /**
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, byte[] key, byte[] value, Header[] headers) {
        return estimateSizeInBytesUpperBound(magic, compressionType, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    /**
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, ByteBuffer key,
                                                    ByteBuffer value, Header[] headers) {
        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            return DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers);
        else if (compressionType != CompressionType.NONE){
            /**
             * 1️⃣ Records.LOG_OVERHEAD = 12 字节
             * 这是 Log 层面的开销，每个 Record 在日志文件中都需要：
             * Offset:  0        8       12
             *          ├────────┼───────┤
             * Field:   │ Offset │ Size  │
             *          │ 8 bytes│4 bytes│
             *          └────────┴───────┘
             * Offset (8字节)：消息在分区中的逻辑偏移量
             * Size (4字节)：Record 的长度（不包括 offset 和 size 本身）
             * 2️⃣ LegacyRecord.recordOverhead(magic)
             * 这是 Record 内部的固定头部开销：
             * // V0
             * RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH
             *                    = 6 + 4 + 4 = 14 字节
             *
             * // V1
             * RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH
             *                    = 14 + 4 + 4 = 22 字节
             * 包括：
             * CRC (4B) + Magic (1B) + Attributes (1B) [+ Timestamp (8B for V1)]
             * Key Size (4B) + Value Size (4B)
             * 3️⃣ LegacyRecord.recordSize(magic, key, value)
             *
             * 为什么压缩时要多加一次 overhead？
             * 这是因为 Legacy Record 的压缩机制：
             * 未压缩时：
             * [LOG_OVERHEAD][Record: CRC+Magic+Attr+Key+Value]
             *
             * 压缩时（Wrapper Record）：
             * [LOG_OVERHEAD]
             * [Wrapper Record: CRC+Magic+Attr(压缩标志)]
             *   └─ [内部 MessageSet（压缩数据）]
             *       ├─ [Record1: CRC+Magic+Attr+Key+Value]
             *       ├─ [Record2: CRC+Magic+Attr+Key+Value]
             *       └─ [Record3: CRC+Magic+Attr+Key+Value]
             * 关键点：
             *  外层有一个 Wrapper Record（包装记录），包含压缩标志
             *  内层是实际的 Records（已压缩）
             *  所以需要两层 overhead：
             *   Wrapper Record 的 overhead
             *   内部 Records 的总大小（通过 recordSize 估算）
             * | 层级 | 组成 | 大小 |
             * |------|------|------|
             * | **Log 层** | Offset + Size | 12 字节 |
             * | **Wrapper Record** | CRC + Magic + Attr + ... | 22 字节 (V1) |
             * | **内部 Records** | 压缩后的多个 records | 估算值 |
             *
             * 为什么需要 Wrapper Record？
             *  因为 Legacy 格式的压缩是在 MessageSet 级别，而不是 Batch 级别：
             *    将多个 Records 放入一个 MessageSet
             *    压缩整个 MessageSet
             *    将压缩数据作为 Wrapper Record 的 value
             *    Wrapper Record 的 attributes 标记压缩类型
             * 这就是为什么压缩时需要额外加一层 recordOverhead —— 它代表的是 Wrapper Record 的开销，而不是重复计算。
             */
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic) + LegacyRecord.recordSize(magic, key, value);
        }
        else
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
    }

    /**
     * Return the size of the record batch header.
     *
     * For V0 and V1 with no compression, it's unclear if Records.LOG_OVERHEAD or 0 should be chosen. There is no header
     * per batch, but a sequence of batches is preceded by the offset and size. This method returns `0` as it's what
     * `MemoryRecordsBuilder` requires.
     */
    public static int recordBatchHeaderSizeInBytes(byte magic, CompressionType compressionType) {
        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic);
        } else {
            return 0;
        }
    }


}
