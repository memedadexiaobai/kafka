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
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Checksums;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class represents the serialized key and value along with the associated CRC and other fields
 * of message format versions 0 and 1. Note that it is uncommon to need to access this class directly.
 * Usually it should be accessed indirectly through the {@link Record} interface which is exposed
 * through the {@link Records} object.
 */
public final class LegacyRecord {

    /**
     * The current offset and size for all the fixed-length fields
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;// 0+4=4
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;// 4+1=5
    public static final int ATTRIBUTES_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;//5+1=6
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;//5+1=6
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;// 6+8=14
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;// 6+4=10
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;// 14+4=18
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * The size for the record header
     */
    public static final int HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;// 4+1+1=6
    public static final int HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH;// 4+1+1+8=14

    /**
     * The amount of overhead(开销) bytes in a record
     */
    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;// 6+4+4=14

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;// 14+4+4=22

    /**
     * Legacy Record 结构对比
     * V0 格式（无时间戳）
     * Offset:  0       4    5     6      10   Key结束  Key结束+4  Value结束
     *          ├───────┼────┼─────┼──────┼────┼────────┼────────┼────────┤
     * Field:   │ CRC   │Magi│Attri│(无)  │Key │  Key   │Value   │ Value  │
     *          │       │ c  │butes│      │Size│  Data  │ Size   │  Data  │
     *          │       │    │     │      │    │        │        │        │
     * Size:    │4 bytes│1 B │1 B  │  -   │4 B │ keyLen │ 4 B    │valLen  │
     *          └───────┴────┴─────┴──────┴────┴────────┴────────┴────────┘
     *                                           ↑                    ↑
     *                                       KEY_OFFSET_V0       VALUE开始位置
     *                                       (=10)
     * V1 格式（有时间戳）
     * Offset:  0       4    5     6      14     18   Key结束  Key结束+4  Value结束
     *          ├───────┼────┼─────┼──────┼──────┼────┼────────┼────────┼────────┤
     * Field:   │ CRC   │Magi│Attri│Times │Key   │Key │  Key   │Value   │ Value  │
     *          │       │ c  │butes│tamp  │Size  │Off │  Data  │ Size   │  Data  │
     *          │       │    │     │      │      │set │        │        │        │
     * Size:    │4 bytes│1 B │1 B  │8 B   │4 B   │    │ keyLen │ 4 B    │valLen  │
     *          └───────┴────┴─────┴──────┴──────┴────┴────────┴────────┴────────┘
     *                                                 ↑                    ↑
     *                                             KEY_OFFSET_V1       VALUE开始位置
     *                                             (=18)
     *  | 偏移量 | 字段名 | V0 | V1 | 长度 | 说明 |
     * |--------|--------|----|----|------|------|
     * | **0** | `crc` | ✓ | ✓ | 4 字节 | CRC32 校验码 |
     * | **4** | `magic` | ✓ | ✓ | 1 字节 | 版本标识（0 或 1） |
     * | **5** | `attributes` | ✓ | ✓ | 1 字节 | 压缩类型 + 时间戳类型标志 |
     * | **6** | `timestamp` | ✗ | ✓ | 8 字节 | **仅 V1**：消息时间戳 |
     * | **6/14** | `key_size` | ✓ | ✓ | 4 字节 | key 的长度（-1 表示 null） |
     * | **10/18** | `key` | ✓ | ✓ | 变长 | key 的实际数据 |
     * | **...** | `value_size` | ✓ | ✓ | 4 字节 | value 的长度（-1 表示 null） |
     * | **...** | `value` | ✓ | ✓ | 变长 | value 的实际数据 |
     *
     * 示例1：V0 + 无压缩 + Key="id" + Value="123"
     * Byte:    0       4    5     6      10   11  12  13  14  15  16  17  18
     *          ├───────┼────┼─────┼──────┼────┼───┼───┼───┼───┼───┼───┼───┼───┤
     *          │ CRC   │Magi│Attri│KeySz │'i' │'d'│Val│Siz│'1'│'2'│'3'│   │   │
     *          │0x12345│ c  │butes│=2    │    │   │eSz│=3 │   │   │   │   │   │
     *          │  678  │ 0  │ 0x00│0x00..│0x69│0x64│0x0│0x0│0x31│0x32│0x33│   │
     *          │       │    │     │ ..02 │    │   │0..│0..│    │    │    │   │
     *          │       │    │     │      │    │   │ .3 │ .0 │    │    │    │   │
     *          └───────┴────┴─────┴──────┴────┴───┴───┴───┴───┴───┴───┴───┴───┘
     *           4B     1B   1B    4B     2B(key)  4B    3B(value)
     *
     * CRC 计算范围：从 magic (offset 4) 到 value 结束
     * 示例2：V1 + GZIP + Key=null + Value="hello"
     * Byte:    0       4    5     6              14     18     22   23-27
     *          ├───────┼────┼─────┼──────────────┼──────┼──────┼────┼──────┤
     *          │ CRC   │Magi│Attri│  Timestamp   │KeySz │ValSz │'h' │'ello'│
     *          │0xABCD │ c  │butes│  1234567890  │=-1   │=5    │    │      │
     *          │ EFGH  │ 1  │0x01 │ 0x00000000   │0xFF..│0x00..│0x68│0x65..│
     *          │       │    │     │  004596C2    │ ..FF│ ..05 │    │      │
     *          └───────┴────┴─────┴──────────────┴──────┴──────┴────┴──────┘
     *           4B     1B   1B     8B            4B     4B     5B(value)
     *
     * 注意：
     * - attributes = 0x01 → bit0=1 (GZIP压缩)
     * - key_size = -1 (0xFFFFFFFF) → key 为 null，不存储 key 数据
     * - value 直接跟在 key_size 后面
     * 关键差异：V0 vs V1
     * 1. 时间戳字段
     * V0: 没有时间戳
     *     attributes 中也没有时间戳类型标志
     *
     * V1: 有 8 字节时间戳
     *     attributes 的 bit 3 表示时间戳类型：
     *     - 0 = CREATE_TIME（生产者创建时间）
     *     - 1 = LOG_APPEND_TIME（Broker 追加时间）
     * 2. Key/Value 的偏移量不同
     * // V0
     * KEY_OFFSET_V0 = 10   // 6 (attributes后) + 4 (key_size)
     * VALUE 紧随 key 数据之后
     *
     * // V1
     * KEY_OFFSET_V1 = 18   // 14 (timestamp后) + 4 (key_size)
     * VALUE 紧随 key 数据之后
     * 3. CRC 计算范围
     * // V0 和 V1 相同
     * CRC 覆盖：从 magic (offset 4) 到整个 record 结束
     *
     * // 代码实现
     * checksum.update(buffer, MAGIC_OFFSET, length - MAGIC_OFFSET);
     *
     * 与 V2 (DefaultRecord) 的对比
     * | 特性 | Legacy (V0/V1) | Default (V2+) |
     * |------|----------------|---------------|
     * | **头部位置** | 每个 Record 都有 CRC | CRC 在 RecordBatch 级别 |
     * | **时间戳** | V0 无，V1 有绝对时间戳 | 相对时间戳（delta） |
     * | **Offset** | 不存储（由 Broker 分配） | 相对 offset（delta） |
     * | **Headers** | ❌ 不支持 | ✅ 支持 |
     * | **压缩** | 外层包装（MessageSet） | 内嵌在 Batch 中 |
     * | **最小开销** | V0: 14字节, V1: 22字节 | 约 3-5 字节（varint） |
     *
     * Legacy Record 的设计特点：
     *  ✅ 简单直接：固定字段 + 变长数据
     *  ⚠️ 空间浪费：每个 Record 都有 4 字节 CRC
     *  ⚠️ 功能有限：V0 无时间戳，都不支持 Headers
     *  🔄 版本演进：V1 通过增加 timestamp 字段向后兼容
     * 这就是为什么 Kafka 后来引入了 V2 格式的 DefaultRecord，将元数据移到 Batch 级别，大幅减少了 overhead。
     */

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    private static final byte COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Specify the mask of timestamp type: 0 for CreateTime, 1 for LogAppendTime.
     */
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public LegacyRecord(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    public LegacyRecord(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return crc32(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this record
     */
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum();
    }

    /**
     * Throw an CorruptRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0)
            throw new CorruptRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = " + sizeInBytes() + ")");

        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc), but
     * excluding the log overhead (offset and record size).
     * @return the size in bytes
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     * @return the size in bytes of the key (0 if the key is null)
     */
    public int keySize() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        else
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
    }

    /**
     * Does the record have a key?
     * @return true if so, false otherwise
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        else
            return KEY_OFFSET_V1 + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     * @return the size in bytes of the value (0 if the value is null)
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * Check whether the value field of this record is null.
     * @return true if the value is null, false otherwise
     */
    public boolean hasNullValue() {
        return valueSize() < 0;
    }

    /**
     * The magic value (i.e. message format version) of this record
     * @return the magic value
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this record
     * @return the attributes
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * When magic value is greater than 0, the timestamp of a record is determined in the following way:
     * 1. wrapperRecordTimestampType = null and wrapperRecordTimestamp is null - Uncompressed message, timestamp is in the message.
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME and WrapperRecordTimestamp is not null - Compressed message using LOG_APPEND_TIME
     * 3. wrapperRecordTimestampType = CREATE_TIME and wrapperRecordTimestamp is not null - Compressed message using CREATE_TIME
     *
     * @return the timestamp as determined above
     */
    public long timestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return RecordBatch.NO_TIMESTAMP;
        else {
            // case 2
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null)
                return wrapperRecordTimestamp;
            // Case 1, 3
            else
                return buffer.getLong(TIMESTAMP_OFFSET);
        }
    }

    /**
     * Get the timestamp type of the record.
     *
     * @return The timestamp type or {@link TimestampType#NO_TIMESTAMP_TYPE} if the magic is 0.
     */
    public TimestampType timestampType() {
        return timestampType(magic(), wrapperRecordTimestampType, attributes());
    }

    /**
     * The compression type used with this record
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the value of this record
     * @return the value or null if the value for this record is null
     */
    public ByteBuffer value() {
        return Utils.sizeDelimited(buffer, valueSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     * @return the buffer or null if the key for this record is null
     */
    public ByteBuffer key() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V0);
        else
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V1);
    }

    /**
     * Get the underlying buffer backing this record instance.
     *
     * @return the buffer
     */
    public ByteBuffer buffer() {
        return this.buffer;
    }

    public String toString() {
        if (magic() > 0)
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 timestampType(),
                                 timestamp(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        else
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(LegacyRecord.class))
            return false;
        LegacyRecord record = (LegacyRecord) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    /**
     * Create a new record instance. If the record's compression type is not none, then
     * its value payload should be already compressed with the specified type; the constructor
     * would always write the value payload as is and will not do the compression itself.
     *
     * @param magic The magic value to use
     * @param timestamp The timestamp of the record
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param compressionType The compression type used on the contents of the record (if any)
     * @param timestampType The timestamp type to be used for this record
     */
    public static LegacyRecord create(byte magic,
                                      long timestamp,
                                      byte[] key,
                                      byte[] value,
                                      CompressionType compressionType,
                                      TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new LegacyRecord(buffer);
    }

    public static LegacyRecord create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    /**
     * Write the header for a compressed record set in-place (i.e. assuming the compressed record data has already
     * been written at the value offset in a wrapped record). This lets you dynamically create a compressed message
     * set, and then go back later and fill in its size and CRC, which saves the need for copying to another buffer.
     *
     * @param buffer The buffer containing the compressed record data positioned at the first offset of the
     * @param magic The magic value of the record set
     * @param recordSize The size of the record (including record overhead)
     * @param timestamp The timestamp of the wrapper record
     * @param compressionType The compression type used
     * @param timestampType The timestamp type of the wrapper record
     */
    public static void writeCompressedRecordHeader(ByteBuffer buffer,
                                                   byte magic,
                                                   int recordSize,
                                                   long timestamp,
                                                   CompressionType compressionType,
                                                   TimestampType timestampType) {
        int recordPosition = buffer.position();
        int valueSize = recordSize - recordOverhead(magic);

        // write the record header with a null value (the key is always null for the wrapper)
        write(buffer, magic, timestamp, null, null, compressionType, timestampType);
        buffer.position(recordPosition);

        // now fill in the value size
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // compute and fill the crc from the beginning of the message
        long crc = crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try (DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer))) {
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Write the record data with the given compression type and return the computed crc.
     *
     * @param out The output stream to write to
     * @param magic The magic value to be used
     * @param timestamp The timestamp of the record
     * @param key The record key
     * @param value The record value
     * @param compressionType The compression type
     * @param timestampType The timestamp type
     * @return the computed CRC for this record.
     * @throws IOException for any IO errors writing to the output stream.
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             byte[] key,
                             byte[] value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             ByteBuffer key,
                             ByteBuffer value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }

    /**
     * Write a record using raw fields (without validation). This should only be used in testing.
     */
    public static void write(DataOutputStream out,
                             byte magic,
                             long crc,
                             byte attributes,
                             long timestamp,
                             byte[] key,
                             byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    private static void write(DataOutputStream out,
                              byte magic,
                              long crc,
                              byte attributes,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value) throws IOException {
        if (magic != RecordBatch.MAGIC_VALUE_V0 && magic != RecordBatch.MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        // write crc
        /**
         * 为了将有符号的 int 转换为无符号的 long，确保写入的是正确的 32 位无符号整数值。
         * Java 没有无符号整数类型
         *  Java 的 int 是有符号 32 位，范围是 -2^31 ~ 2^31-1（-2147483648 ~ 2147483647）。
         *  但 CRC32 校验码是一个无符号 32 位整数，范围应该是 0 ~ 2^32-1（0 ~ 4294967295）。
         * CRC 计算的结果
         *  long crc = checksum.getValue();  // 返回 long 类型
         * 虽然返回值是 long（64位），但实际有效数据只有低32位。高32位可能是：
         *  0（如果 CRC 值 < 2^31）
         *  全1（如果 CRC 值 >= 2^31，因为有符号扩展）
         *
         * 明确语义 + 防止高位污染
         * 原因1：清除高32位垃圾数据
         * 虽然 checksum.getValue() 理论上只使用低32位，但防御性编程确保高32位一定是0：
         * crc = 0xFFFFFFFF12345678L;  // 假设高32位有垃圾数据（理论上不会）
         *
         * // ❌ 直接转换
         * (int) crc = 0x12345678;  // 只取低32位，但如果期望的是完整值就错了
         *
         * // ✅ 先掩码再转换
         * crc & 0xffffffffL = 0x0000000012345678L;  // 明确清除高32位
         * (int) (crc & 0xffffffffL) = 0x12345678;
         * 原因2：类型转换的明确意图
         * // ❌ 不明确：读者可能疑惑为什么 long 转 int
         * out.writeInt((int) crc);
         *
         * // ✅ 明确：这是在处理无符号32位整数
         * out.writeInt((int) (crc & 0xffffffffL));
         * //          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         * //          明确表示"取低32位作为无符号数"
         * 原因3：与 Checksum API 保持一致
         * 看 Java 的 Checksum.getValue() 文档：
         *  Returns the current checksum value as a long.
         * 虽然 CRC32 只有32位，但接口统一返回 long（为了兼容 CRC64 等更大的校验和）
         * 标准用法就是：
         * long crc = checksum.getValue();
         * int unsignedCrc = (int) (crc & 0xffffffffL);  // 提取无符号32位
         * crc & 0xffffffffL
         *
         * 0xffffffffL = 0x00000000FFFFFFFF
         *             = 00000000 00000000 00000000 00000000 11111111 11111111 11111111 11111111
         *
         * 示例：crc = 0x123456789ABCDEF0L
         *       0x123456789ABCDEF0
         *     & 0x00000000FFFFFFFF
         *     = 0x000000009ABCDEF0  ← 高32位清零，低32位保留
         *
         * 然后 (int) 0x000000009ABCDEF0L = 0x9ABCDEF0
         *
         * 对比：有无掩码的区别
         * | CRC 值 | `(int) crc` | `(int) (crc & 0xffffffffL)` | 是否相同 |
         * |--------|-------------|----------------------------|---------|
         * | 0x00000001 | 1 | 1 | ✓ |
         * | 0x7FFFFFFF | 2147483647 | 2147483647 | ✓ |
         * | 0x80000000 | -2147483648 | -2147483648 | ✓ (二进制相同) |
         * | 0xFFFFFFFF | -1 | -1 | ✓ (二进制相同) |
         * | **0x1FFFFFFFF** | 0xFFFFFFFF (-1) | **0xFFFFFFFF (-1)** | ✓ |
         * 关键发现：在二进制层面，两者结果确实相同！因为 (int) 强制转换本身就会截断为低32位。
         * 那为什么还要加掩码？
         * 1. 代码自文档化（Self-documenting）
         * // 看到 & 0xffffffffL，立即明白：
         * // "这是在处理无符号32位整数"
         * (int) (crc & 0xffffffffL)
         *
         * // 而这样写需要思考一下：
         * // "为什么 long 转 int？会不会丢失数据？"
         * (int) crc
         * 2. 与其他语言/库保持一致
         * 在 C/C++、Python 等语言中，处理无符号整数时常用这种模式：
         * 3. 防止未来修改引入 Bug
         * 如果将来 checksum.getValue() 的实现改变（比如返回的值包含高位信息），有掩码保护更安全。
         * 4. Kafka 代码规范
         * 查看 Kafka 其他地方对 CRC 的处理，都是统一模式：这是一种团队约定的最佳实践。
         *
         *
         * `& 0xffffffffL` 的作用：
         *
         * | 作用 | 说明 |
         * |------|------|
         * | **语义明确** | 表明这是无符号32位整数转换 |
         * | **防御性编程** | 确保高32位被清零 |
         * | **代码一致性** | 与 Kafka 其他部分保持统一风格 |
         * | **实际效果** | 二进制层面与直接 `(int)` 转换相同 |
         *
         * **简单来说**：虽然技术上可以省略，但加上它是**更好的编程实践**，让代码意图更清晰，符合 Kafka 处理无符号整数的标准模式。
         */
        out.writeInt((int) (crc & 0xffffffffL));
        // write magic value
        out.writeByte(magic);
        // write attributes
        out.writeByte(attributes);

        // maybe write timestamp
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            out.writeLong(timestamp);

        // write the key
        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            Utils.writeTo(out, key, size);
        }
        // write the value
        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            Utils.writeTo(out, value, size);
        }
    }

    static int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
        return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
    }

    public static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    /**
     * 计算 Legacy Record（旧版本消息格式）的 attributes 字节。这个字节是一个位字段（bit field），用不同的位来存储多个标志信息。
     * 在 Kafka 的早期版本（v0 和 v1）中，每个消息都有一个 attributes 字节，用于存储：
     *   压缩编解码器类型（低2位）
     *   时间戳类型（第3位，仅 v1 支持
     *
     * Attributes 字节结构
     * Magic V0 格式
     * Bit:  7 6 5 4 3 2 1 0
     *       ────────────────
     *       ? ? ? ? ? C C C
     *                 ^ ^ ^
     *                 压缩编解码器 (0-3)
     * Magic V1 格式
     * Bit:  7 6 5 4 3 2 1 0
     *       ────────────────
     *       ? ? ? ? T ? C C
     *               ^   ^ ^
     *               |   └─ 压缩编解码器 (0-3)
     *               └───── 时间戳类型 (0=CREATE_TIME, 1=LOG_APPEND_TIME)
     *
     * 为什么用位运算？
     * | 优势 | 说明 |
     * |------|------|
     * | **节省空间** | 1个字节存储多个标志 |
     * | **高效读写** | 位运算速度极快 |
     * | **向后兼容** | 新版本可以复用保留位 |
     * | **协议标准** | 网络协议的常见做法 |
     *
     * 与 V2+ 格式的对比
     * 在 V2 格式（当前主流）中：
     *  attributes 扩展为 2 字节
     *  支持更多标志（事务控制、幂等性等）
     *  压缩信息移到 RecordBatch 级别，而非单个 Record
     * V2 Attributes 结构：
     * Bit:  15 14 13 12 11 10 9  8  7  6  5  4  3  2  1  0
     *       ───────────────────────────────────────────────
     *       ?  ?  ?  ?  ?  ?  ?  ?  ?  ?  ?  ?  T  C  C  C
     *                                           ^  ^  ^  ^
     *                                           |  └───── 压缩
     *                                           └──────── 时间戳
     * 这段代码的核心逻辑：
     *  低2位：存储压缩编解码器类型（GZIP/SNAPPY/LZ4/NONE）
     *  第3位（仅 v1）：存储时间戳类型（CREATE_TIME/LOG_APPEND_TIME）
     *  位运算组合：用 |= 将不同标志合并到一个字节
     * 这是典型的紧凑协议设计，在有限的空间内编码尽可能多的信息。虽然现在主要使用 V2 格式，但理解 V0/V1 对于处理历史数据和协议演进很有帮助。
     */
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        // 1️⃣ 初始化 attributes
        byte attributes = 0;
        if (type.id > 0) {
            /**
             * 2️⃣ 设置压缩编解码器
             * 含义：如果有压缩，将压缩类型写入低2位。
             * 位运算解析：
             * COMPRESSION_CODEC_MASK = 0x03 = 00000011（掩码，只保留低2位）
             * type.id：压缩类型的 ID
             * 0 = NONE（无压缩）
             * 1 = GZIP
             * 2 = SNAPPY
             * 3 = LZ4
             * & 操作：确保只取低2位（防止非法值）
             * |= 操作：将结果写入 attributes 的低2位
             * // GZIP 压缩 (type.id = 1)
             * attributes = 0;
             * attributes |= (0x03 & 1) = 0x01
             * // 二进制: 00000001
             * //          ^^
             * //          压缩类型=1 (GZIP)
             *
             * // LZ4 压缩 (type.id = 3)
             * attributes |= (0x03 & 3) = 0x03
             * // 二进制: 00000011
             * //          ^^
             * //          压缩类型=3 (LZ4)
             */
            attributes |= (byte) (COMPRESSION_CODEC_MASK & type.id);
        }
        /**
         * 3️⃣ 设置时间戳类型
         * 背景：
         *  v0 版本：不支持时间戳
         *  v1 版本（magic=1）：引入了时间戳，但需要指定类型
         * 两种时间戳类型：
         * | 类型 | 含义 | attributes 的第2位 |
         * |------|------|-------------------|
         * | CREATE_TIME | 消息创建时间 | 0 | | LOG_APPEND_TIME | Broker 追加时间 | 1 |
         * 位运算：
         * TIMESTAMP_TYPE_MASK = 0x08 = 00001000（第3位，从0开始计数）
         * 如果是 LOG_APPEND_TIME，将该位置为 1
         * // v1 + GZIP + LOG_APPEND_TIME
         * attributes = 0x01;  // 低2位：GZIP
         * attributes |= 0x08; // 第3位：LOG_APPEND_TIME
         * // 最终: 00001001 = 0x09
         * //       ^^^^
         * //       ||||
         * //       |||└─ bit 0: 压缩类型低位
         * //       ||└── bit 1: 压缩类型高位
         * //       |└─── bit 2: 保留
         * //       └──── bit 3: 时间戳类型 (1=LOG_APPEND_TIME)
         */
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
                throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for " +
                        "message format v1");
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                attributes |= TIMESTAMP_TYPE_MASK;
        }
        return attributes;
    }

    // visible only for testing
    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    private static long crc32(ByteBuffer buffer, int offset, int size) {
        CRC32 crc = new CRC32();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        CRC32 crc = new CRC32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            Checksums.updateLong(crc, timestamp);
        // update for the key
        if (key == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = key.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, key, size);
        }
        // update for the value
        if (value == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = value.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, value, size);
        }
        return crc.getValue();
    }

    static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        else if (magic == 1)
            return RECORD_OVERHEAD_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    static int headerSize(byte magic) {
        if (magic == 0)
            return HEADER_SIZE_V0;
        else if (magic == 1)
            return HEADER_SIZE_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        else if (magic == 1)
            return KEY_OFFSET_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    public static TimestampType timestampType(byte magic, TimestampType wrapperRecordTimestampType, byte attributes) {
        if (magic == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else if (wrapperRecordTimestampType != null)
            return wrapperRecordTimestampType;
        else
            return (attributes & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

}
