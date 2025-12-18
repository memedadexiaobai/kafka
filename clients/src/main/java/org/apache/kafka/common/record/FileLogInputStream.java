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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.LegacyFileChannelRecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch.DefaultFileChannelRecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Objects;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * A log input stream which is backed by a {@link FileChannel}.
 */
public class FileLogInputStream implements LogInputStream<FileLogInputStream.FileChannelRecordBatch> {
    private int position;
    private final int end;
    private final FileRecords fileRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /**
     * Create a new log input stream over the FileChannel
     * @param records Underlying FileRecords instance
     * @param start Position in the file channel to start from
     * @param end Position in the file channel not to read past
     */
    FileLogInputStream(FileRecords records,
                       int start,
                       int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
    }

    /**
     * 从磁盘字节流里一个接一个地把 RecordBatch 对象挖出来” 的底层迭代器实现——
     *      零拷贝、逐条校验、自动区分新老格式，直到文件末尾返回 null。
     *
     *  position → 读17B头 → 解析offset/size → 校验大小 → 判magic → 建batch → position前移
     *      ↑        ↑            ↑           ↑         ↑         ↑
     *   边界 guard  固定常量   最小合法     剩余足够   格式分支  迭代推进
     *
     * nextBatch() 就是 “磁盘字节流 → RecordBatch 对象” 的 最小化、零拷贝、格式自适应 迭代器：
     *  读17B头 → 校验 → 按magic分流 → 推进position，
     *  每条消息一个循环，直到文件尾优雅返回 null。
     *
     * @return
     * @throws IOException
     */
    @Override
    public FileChannelRecordBatch nextBatch() throws IOException {
        FileChannel channel = fileRecords.channel();
        // 剩余字节不够 17 字节固定头 → 肯定没完整消息了，直接结束。
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC)
            return null;

        // 一次 DMA 读 17 字节 进 logHeaderBuffer，后续全部 按偏移量常量 解析，零拷贝。
        logHeaderBuffer.rewind();
        Utils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(OFFSET_OFFSET); // offset：这条消息在分区里的逻辑序号
        int size = logHeaderBuffer.getInt(SIZE_OFFSET); // size：整条消息体长度（不含这 12 字节头）

        // V0 has the smallest overhead, stricter checking is done later 最小合法体积过滤，防止后续读越界或 SIGBUS
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                            "overhead (%d) in file %s.", size, LegacyRecord.RECORD_OVERHEAD_V0, fileRecords.file()));

        // 剩余文件不够装下整条消息 → 文件尾巴腐败，优雅结束迭代。
        if (position > end - LOG_OVERHEAD - size)
            return null;

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        final FileChannelRecordBatch batch;

        // 返回的 batch 对象内部只持有 (offset, magic, fileChannel, position, size)——仍然零拷贝，真正的字节解析延迟到第一次访问时才发生。
        if (magic < RecordBatch.MAGIC_VALUE_V2) // magic=0/1 → 老版 消息级格式（Legacy）
            batch = new LegacyFileChannelRecordBatch(offset, magic, fileRecords, position, size);
        else // magic=2 → 新版 批量格式（RecordBatch，支持事务、压缩、幂等）
            batch = new DefaultFileChannelRecordBatch(offset, magic, fileRecords, position, size);

        // position 永远指向下一条消息头起始，下次循环继续
        position += batch.sizeInBytes();
        return batch;
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the record batches
     * without needing to read the record data into memory until it is needed. The downside
     * is that entries will generally no longer be readable when the underlying channel is closed.
     */
    public abstract static class FileChannelRecordBatch extends AbstractRecordBatch {
        protected final long offset;
        protected final byte magic;
        protected final FileRecords fileRecords;
        protected final int position;
        protected final int batchSize;

        private RecordBatch fullBatch;
        private RecordBatch batchHeader;

        FileChannelRecordBatch(long offset,
                               byte magic,
                               FileRecords fileRecords,
                               int position,
                               int batchSize) {
            this.offset = offset;
            this.magic = magic;
            this.fileRecords = fileRecords;
            this.position = position;
            this.batchSize = batchSize;
        }

        @Override
        public CompressionType compressionType() {
            return loadBatchHeader().compressionType();
        }

        @Override
        public TimestampType timestampType() {
            return loadBatchHeader().timestampType();
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public long maxTimestamp() {
            return loadBatchHeader().maxTimestamp();
        }

        public int position() {
            return position;
        }

        @Override
        public byte magic() {
            return magic;
        }

        @Override
        public Iterator<Record> iterator() {
            return loadFullBatch().iterator();
        }

        @Override
        public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
            return loadFullBatch().streamingIterator(bufferSupplier);
        }

        @Override
        public boolean isValid() {
            return loadFullBatch().isValid();
        }

        @Override
        public void ensureValid() {
            loadFullBatch().ensureValid();
        }

        @Override
        public int sizeInBytes() {
            return LOG_OVERHEAD + batchSize;
        }

        @Override
        public void writeTo(ByteBuffer buffer) {
            FileChannel channel = fileRecords.channel();
            try {
                int limit = buffer.limit();
                buffer.limit(buffer.position() + sizeInBytes());
                Utils.readFully(channel, buffer, position);
                buffer.limit(limit);
            } catch (IOException e) {
                throw new KafkaException("Failed to read record batch at position " + position + " from " + fileRecords, e);
            }
        }

        protected abstract RecordBatch toMemoryRecordBatch(ByteBuffer buffer);

        protected abstract int headerSize();

        protected RecordBatch loadFullBatch() {
            if (fullBatch == null) {
                batchHeader = null;
                fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
            }
            return fullBatch;
        }

        protected RecordBatch loadBatchHeader() {
            if (fullBatch != null)
                return fullBatch;

            if (batchHeader == null)
                batchHeader = loadBatchWithSize(headerSize(), "record batch header");

            return batchHeader;
        }

        private RecordBatch loadBatchWithSize(int size, String description) {
            FileChannel channel = fileRecords.channel();
            try {
                ByteBuffer buffer = ByteBuffer.allocate(size);
                Utils.readFullyOrFail(channel, buffer, position, description);
                buffer.rewind();
                return toMemoryRecordBatch(buffer);
            } catch (IOException e) {
                throw new KafkaException("Failed to load record batch at position " + position + " from " + fileRecords, e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FileChannelRecordBatch that = (FileChannelRecordBatch) o;

            FileChannel channel = fileRecords == null ? null : fileRecords.channel();
            FileChannel thatChannel = that.fileRecords == null ? null : that.fileRecords.channel();

            return offset == that.offset &&
                    position == that.position &&
                    batchSize == that.batchSize &&
                    Objects.equals(channel, thatChannel);
        }

        @Override
        public int hashCode() {
            FileChannel channel = fileRecords == null ? null : fileRecords.channel();

            int result = Long.hashCode(offset);
            result = 31 * result + (channel != null ? channel.hashCode() : 0);
            result = 31 * result + position;
            result = 31 * result + batchSize;
            return result;
        }

        @Override
        public String toString() {
            return "FileChannelRecordBatch(magic: " + magic +
                    ", offset: " + offset +
                    ", size: " + batchSize + ")";
        }
    }
}
