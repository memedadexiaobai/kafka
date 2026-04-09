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
package org.apache.kafka.common.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 *
 * The implementation of these methods has been tuned for JVM and the empirical calculations could be found
 * using ByteUtilsBenchmark.java
 */
public final class ByteUtils {

    public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Read a big-endian integer from a byte array
     */
    public static int readIntBE(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xFF) << 24)
            | ((buffer[offset + 1] & 0xFF) << 16)
            | ((buffer[offset + 2] & 0xFF) << 8)
            | (buffer[offset + 3] & 0xFF);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * The implementation is based on Netty's decoding of varint.
     * @see <a href="https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73">Netty's varint decoding</a>
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buffer.get()) << 28;
                        if (tmp < 0) {
                            throw illegalVarintException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * The implementation is based on Netty's decoding of varint.
     * @see <a href="https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73">Netty's varint decoding</a>
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link InputStream} throws {@link IOException}
     * @throws EOFException             if {@link InputStream} throws {@link EOFException}
     */
    static int readUnsignedVarint(InputStream in) throws IOException {
        byte tmp = (byte) in.read();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = (byte) in.read()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = (byte) in.read()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = (byte) in.read()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = (byte) in.read()) << 28;
                        if (tmp < 0) {
                            throw illegalVarintException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarint(InputStream in) throws IOException {
        int value = readUnsignedVarint(in);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarlong(InputStream in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.read()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long raw =  readUnsignedVarlong(buffer);
        return (raw >>> 1) ^ -(raw & 1);
    }

    // visible for testing
    static long readUnsignedVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param in The input to read from
     * @return The double value read
     */
    public static double readDouble(DataInput in) throws IOException {
        return in.readDouble();
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    public static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * Implementation copied from https://github.com/astei/varint-writing-showdown/tree/dev (MIT License)
     * @see <a href="https://github.com/astei/varint-writing-showdown/blob/6b1a4baec4b1f0ce65fa40cf0b282ec775fdf43e/src/jmh/java/me/steinborn/varintshowdown/res/SmartNoDataDependencyUnrolledVarIntWriter.java#L8"> Sample implementation </a>
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) (value & 0x7F | 0x80));
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                buffer.put((byte) ((value >>> 7) & 0xFF));
            } else {
                buffer.put((byte) ((value >>> 7) & 0x7F | 0x80));
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    buffer.put((byte) ((value >>> 14) & 0xFF));
                } else {
                    buffer.put((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        buffer.put((byte) ((value >>> 21) & 0xFF));
                    } else {
                        buffer.put((byte) ((value >>> 21) & 0x7F | 0x80));
                        buffer.put((byte) ((value >>> 28) & 0xFF));
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     * 
     * For implementation notes, see {@link #writeUnsignedVarint(int, ByteBuffer)}
     *
     * 这是 Unsigned Varint 的具体编码实现，采用了手动展开循环的优化方式。
     *  核心原理：Varint 编码规则
     *  每个字节的结构：
     *  最高位(MSB): continuation bit
     *    1 = 还有后续字节
     *    0 = 最后一个字节
     *  低7位: 实际数据（小端序）
     *
     * 值 300 (二进制: 1 0010 1100)
     *
     * 第1字节: 0xAC = 1010 1100
     *          ↑       ↑↑↑↑↑↑↑
     *          1       0101100  (低7位，继续标志=1)
     *
     * 第2字节: 0x02 = 0000 0010
     *          ↑       ↑↑↑↑↑↑↑
     *          0       0000010  (低7位，继续标志=0，结束)
     *
     * 解码: 0101100 + (0000010 << 7) = 44 + 256 = 300 ✓
     * 低7位: 实际数据（小端序）
     *
     * 为什么用 if-else 嵌套而不是循环？
     *  性能优化：避免循环开销和分支预测失败。
     *  优势：
     *   无循环：减少跳转指令
     *   早期退出：小数字快速路径
     *   CPU 流水线友好：分支可预测
     *
     * value = 300
     *
     * ┌─ (value & 0xFFFFFF80) == 0? ─No→ 写第1字节: 0xAC (continuation=1)
     * │                                   │
     * │                                   ├─ (value & 0xFFFFC000) == 0? ─Yes→ 写第2字节: 0x02 (结束)
     * │                                   │                                    ↓
     * │                                   │                                 [0xAC, 0x02] ✓
     * │                                   │
     * │                                   No→ 继续...
     *
     *| 方法 | 操作次数（value=100） | 操作次数（value=300） | 分支预测 |
     * |------|---------------------|---------------------|---------|
     * | 循环法 | 2次迭代+2次判断 | 2次迭代+2次判断 | ⚠️ 可能失败 |
     * | **手动展开** | **1次判断+1次写入** | **2次判断+2次写入** | ✅ 高度可预测 |
     * 对于小数字（大部分场景），手动展开更快！
     * 这个实现的核心思想：
     *  嵌套 if-else 替代循环：避免循环开销
     *  从高位快速判断：用位掩码一次性检查多个位
     *  早期退出：小数字走快速路径
     *  精确控制：每个字节单独处理，清晰设置 continuation(延续) bit
     * 这是典型的空间换时间优化，在 Kafka 这种高性能场景中非常必要。代码虽然看起来复杂，但执行效率远高于通用循环实现。
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeUnsignedVarint(int value, DataOutput out) throws IOException {
        /**
         * 判断是否需要1个字节
         * 含义：检查第7位及以上是否全为0。
         * 0xFFFFFFFF << 7 = 0xFFFFFF80 = 11111111 11111111 11111111 10000000
         * value = 100 (0x64):
         *   00000000 00000000 00000000 01100100
         * & 11111111 11111111 11111111 10000000
         * = 00000000 00000000 00000000 00000000  → 只需1字节 ✓
         *
         * value = 200 (0xC8):
         *   00000000 00000000 00000000 11001000
         * & 11111111 11111111 11111111 10000000
         * = 00000000 00000000 00000000 10000000  → 需要更多字节
         * 范围：0 ~ 127 (0x7F)
         */
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            out.writeByte(value);
        } else {
            /**
             * 1. (value & 0x7F | 0x80) - 写非最后字节
             * 提取低7位 + 设置 continuation bit
             *
             * value = 300 (第1字节):
             *   300 & 0x7F = 0b01001100
             *   | 0x80     = 0b11001100 = 0xAC
             *                ↑
             *            continuation=1
             */
            out.writeByte(value & 0x7F | 0x80);// 写第1字节（设置continuation bit)
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                /**
                 * 第1字节:
                 *   300 & 0x7F = 0100 1100 & 0111 1111 = 0100 1100
                 *   | 0x80     = 0100 1100 | 1000 0000 = 1100 1100 = 0xAC ✓
                 *
                 * 判断是否2字节:
                 *   0xFFFFFFFF << 14 = 0xFFFFC000
                 *   300 & 0xFFFFC000 = 0 → 是！只需2字节
                 *
                 * 第2字节:
                 *   300 >>> 7 = 2 (0x02) ✓
                 *
                 * 最终: [0xAC, 0x02]
                 * 范围：128 ~ 16383 (0x7FF)
                 * 2. (value >>> 7) - 写最后字节
                 * 右移7位，不设置 continuation bit
                 *
                 * value = 300 (第2字节):
                 *   300 >>> 7 = 0b00000010 = 0x02
                 *   ↑
                 *   没有 | 0x80，因为是最后字节
                 */
                out.writeByte(value >>> 7); // 写第2字节（最后字节，不设continuation bit）
            } else {
                out.writeByte((value >>> 7) & 0x7F | 0x80);
                /**
                 * 3. (value & (0xFFFFFFFF << N)) - 快速判断范围
                 * 检查 N 位以上是否全为0
                 * N=7:  判断 0-127      (1字节)
                 * N=14: 判断 0-16383    (2字节)
                 * N=21: 判断 0-2097151  (3字节)
                 * N=28: 判断 0-268435455 (4字节)
                 * 其他: 需要5字节
                 */
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    /**
                     * 判断是否需要3字节
                     * 第1字节: 20000 & 0x7F | 0x80 = 0x80 | 0x80 = 0xA0
                     * 第2字节: (20000 >>> 7) & 0x7F | 0x80 = 0x27 | 0x80 = 0xA7
                     * 判断: 20000 & 0xFFE00000 = 0 → 只需3字节
                     * 第3字节: 20000 >>> 14 = 1 (0x01)
                     *
                     * 最终: [0xA0, 0xA7, 0x01]
                     * 验证: 0x20 + (0x27 << 7) + (0x01 << 14) = 32 + 3200 + 16384 = 19616...
                     * 等等，让我重新算：
                     * 20000 = 0x4E20
                     * 第1字节: 0x4E20 & 0x7F = 0x20, | 0x80 = 0xA0
                     * 第2字节: (0x4E20 >>> 7) & 0x7F = 0x9C & 0x7F = 0x1C, | 0x80 = 0x9C
                     * 第3字节: 0x4E20 >>> 14 = 0x01
                     *
                     * 最终: [0xA0, 0x9C, 0x01]
                     * 解码: 0x20 + (0x1C << 7) + (0x01 << 14) = 32 + 3584 + 16384 = 20000 ✓
                     * 范围：16384 ~ 2097151 (0x1FFFFF)
                     */
                    out.writeByte(value >>> 14);
                } else {
                    /**
                     * 4字节和5字节情况
                     * 同理类推，最多支持 5 个字节（因为 int 是 32 位，32/7 ≈ 4.57，向上取整为 5）
                     */
                    out.writeByte((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        out.writeByte(value >>> 21);
                    } else {
                        out.writeByte((value >>> 21) & 0x7F | 0x80);
                        out.writeByte(value >>> 28);
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarint(int value, DataOutput out) throws IOException {
        /**
         * 这是 ZigZag 编码，用于将有符号整数高效地编码为无符号 varint。
         * 为什么叫 "ZigZag"？
         *  因为映射关系像锯齿形：
         *  有符号轴:  ... -3  -2  -1   0   1   2   3 ...
         *             \   /   \   /   \   /   \   /
         *  无符号轴:  ...  5   3   1   0   2   4   6 ...
         *               ↖ ↗   ↖ ↗   ↖ ↗   ↖ ↗
         *               ZigZag 编码
         * 问题背景
         * 普通 Varint 的问题:Varint 对小正数很高效，但对负数非常低效
         *  // 正数 1
         *  二进制: 00000000 00000000 00000000 00000001
         *  Varint: 0x01  (1字节) ✓ 高效
         *
         *  // 负数 -1
         *  二进制: 11111111 11111111 11111111 11111111  (补码表示)
         *  Varint: 0xFF 0xFF 0xFF 0xFF 0x0F  (5字节!) ❌ 低效
         * 问题：负数在计算机中用补码表示，高位全是 1，导致 varint 需要 5 个字节！
         * 但实际上，我们常见的负数（如 -1, -2, -100）都是小数字，应该也能用少量字节编码。
         *
         * ZigZag 编码的解决方案
         *  核心思想:将有符号整数映射为无符号整数，让小绝对值的数（包括负数）都变成小的无符号数
         *  映射规则：
         *  有符号数 → 无符号数
         *    0     →    0
         *   -1     →    1
         *    1     →    2
         *   -2     →    3
         *    2     →    4
         *   -3     →    5
         *    3     →    6
         *   ...
         *  规律：绝对值小的数（无论正负）都映射为小的无符号数。
         *
         * 步骤1：(value << 1) - 左移1位 相当于乘以2，但保留了符号位信息：
         * value = 5:
         *   原值:   00000000 00000000 00000000 00000101
         *   << 1:   00000000 00000000 00000000 00001010  (= 10)
         *
         * value = -5:
         *   原值:   11111111 11111111 11111111 11111011
         *   << 1:   11111111 11111111 11111111 11110110  (= -10，溢出忽略)
         * 步骤2：(value >> 31) - 算术右移31位 这是关键！算术右移会复制符号位：
         *  value = 5 (正数):
         *   原值:   00000000 00000000 00000000 00000101
         *   >> 31:  00000000 00000000 00000000 00000000  (= 0)
         *           ↑ 符号位是0，右移补0
         *
         * value = -5 (负数):
         *   原值:   11111111 11111111 11111111 11111011
         *   >> 31:  11111111 11111111 11111111 11111111  (= -1，全1)
         *           ↑ 符号位是1，右移补1
         *  结果：
         *   正数 → 0 (0x00000000)
         *   负数 → -1 (0xFFFFFFFF)
         * 步骤3：XOR 异或运算
         *  value = 5:
         *   (5 << 1) ^ 0 = 10 ^ 0 = 10
         *  value = -5:
         *   (-5 << 1) ^ 0xFFFFFFFF
         *
         *   -5 << 1 = 11111111 11111111 11111111 11110110
         *   XOR with: 11111111 11111111 11111111 11111111
         *   =         00000000 00000000 00000000 00001001  (= 9)
         *  XOR 全1的效果 = 按位取反！
         *
         * | value | value << 1 | value >> 31 | (<<1) ^ (>>31) | 最终无符号值 |
         * |-------|-----------|-------------|----------------|------------|
         * | 0 | 0 | 0 | 0 ^ 0 | **0** |
         * | -1 | -2 (0xFFFFFFFE) | -1 (0xFFFFFFFF) | 0xFFFFFFFE ^ 0xFFFFFFFF | **1** |
         * | 1 | 2 | 0 | 2 ^ 0 | **2** |
         * | -2 | -4 (0xFFFFFFFC) | -1 (0xFFFFFFFF) | 0xFFFFFFFC ^ 0xFFFFFFFF | **3** |
         * | 2 | 4 | 0 | 4 ^ 0 | **4** |
         * | -3 | -6 (0xFFFFFFFA) | -1 (0xFFFFFFFF) | 0xFFFFFFFA ^ 0xFFFFFFFF | **5** |
         * | 3 | 6 | 0 | 6 ^ 0 | **6** |
         *
         * 解码过程（反向操作）
         * encoded = 9 (对应原始值 -5):
         *   value >>> 1 = 9 >>> 1 = 4
         *   value & 1   = 9 & 1 = 1
         *   -(1)        = -1 = 0xFFFFFFFF
         *   4 ^ 0xFFFFFFFF = 0xFFFFFFFB = -5 ✓
         *
         * 这个计算的目的是：
         *  解决负数 varint 编码低效的问题
         *  让小绝对值的负数也能用少量字节表示
         *  通过位运算高效实现（无需条件分支）
         * 应用场景：Kafka 协议中所有可能为负数的字段（如 timestamp delta、offset delta 等）都使用 ZigZag + Varint 编码。
         */
        writeUnsignedVarint((value << 1) ^ (value >> 31), out);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     *
     * 这是 Long 类型的 ZigZag + Varint 编码，但采用了循环实现（而非像 int 那样手动展开）。
     * 关键问题：为什么 Long 用循环，而 Int 用手动展开？
     * 这是一个权衡设计！
     *  Int 版本（手动展开）的优势
     *  // Int 最多 5 字节，嵌套深度固定且较浅
     * if (...) {
     *     // 1字节
     * } else {
     *     if (...) {
     *         // 2字节
     *     } else {
     *         if (...) {
     *             // 3字节
     *         } else {
     *             if (...) {
     *                 // 4字节
     *             } else {
     *                 // 5字节
     *             }
     *         }
     *     }
     * }
     * 适合手动展开的原因：
     *  最多 5 层嵌套，代码复杂度可控
     *  常见场景（小数字）快速退出
     *  性能收益明显
     * Long 版本（循环）的原因
     * // Long 最多 10 字节！
     * // 如果手动展开会有 10 层嵌套...
     * if (...) {
     *     // 1字节
     * } else {
     *     if (...) {
     *         // 2字节
     *     } else {
     *         // ... 重复10次！代码会变得非常冗长
     *     }
     * }
     * | 因素 | 说明 |
     * |------|------|
     * | **代码可维护性** | 10层嵌套太深，难以阅读和维护 |
     * | **边际收益递减** | Long 的大值场景较少，优化收益不如 int 明显 |
     * | **代码复用性** | 循环结构更通用，易于理解 |
     * | **JIT 优化** | 现代 JVM 会对热点循环做优化（如循环展开） |
     *
     * 这个实现的"优化"体现在：
     *   ✅ ZigZag 编码：让负数也能高效编码
     *   ⚠️ 使用循环而非展开：在性能和代码简洁性之间做了权衡
     *   🔧 可进一步优化：如果需要，可以部分展开前几层
     * 设计哲学：
     *   Int 高频使用 → 极致优化（手动展开）
     *   Long 相对低频 → 平衡设计（循环实现）
     * 如果你发现 Long 编码成为瓶颈，可以考虑改为部分展开的实现。但在大多数 Kafka 场景中，当前的循环实现已经足够高效了。
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarlong(long value, DataOutput out) throws IOException {
        /**
         * 1️⃣ ZigZag 编码
         * | 类型 | ZigZag 公式 | 右移位数 |
         * |------|------------|---------|
         * | int (32位) | `(value << 1) ^ (value >> 31)` | 31 |
         * | long (64位) | `(value << 1) ^ (value >> 63)` | 63 |
         * 为什么是 63？
         *  long 是 64 位，符号位在第 63 位（从0开始计数）
         *  算术右移 63 位后，正数得到 0，负数得到 -1（全1)
         * value = -1L:
         *   -1L << 1   = 0xFFFFFFFFFFFFFFFE
         *   -1L >> 63  = 0xFFFFFFFFFFFFFFFF  (-1)
         *   XOR        = 0x0000000000000001  (= 1) ✓
         *
         * value = 1L:
         *   1L << 1    = 0x0000000000000002
         *   1L >> 63   = 0x0000000000000000  (0)
         *   XOR        = 0x0000000000000002  (= 2) ✓
         */
        long v = (value << 1) ^ (value >> 63);
        /**
         * 2️⃣ Varint 编码循环
         * 判断条件：v & 0xffffffffffffff80L
         *  0xffffffffffffff80L = 低7位为0，高57位全为1
         *  如果结果不为0，说明第7位及以上还有数据，需要继续编码
         * 示例：value = -1L → ZigZag后 v = 1
         * 第1轮:
         *   v = 1
         *   v & 0xffffffffffffff80L = 0 → 不进入循环
         *   直接写: out.writeByte(1)  // [0x01] ✓ 只需1字节
         * 示例：value = -64L → ZigZag后 v = 127
         * 第1轮:
         *   v = 127 = 0x7F
         *   v & 0xffffffffffffff80L = 0 → 不进入循环
         *   直接写: out.writeByte(127)  // [0x7F] ✓ 只需1字节
         * 示例：value = -65L → ZigZag后 v = 129
         * 第1轮:
         *   v = 129 = 0x81
         *   v & 0xffffffffffffff80L = 0x80 ≠ 0 → 进入循环
         *   写: (129 & 0x7f) | 0x80 = 0x01 | 0x80 = 0x81
         *   v >>>= 7 → v = 1
         *
         * 第2轮:
         *   v = 1
         *   v & 0xffffffffffffff80L = 0 → 退出循环
         *   写: out.writeByte(1)
         *
         * 最终: [0x81, 0x01] ✓
         */
        while ((v & 0xffffffffffffff80L) != 0L) {
            out.writeByte(((int) v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        writeUnsignedVarlong(v, buffer);
    }

    // visible for testing and benchmarking
    public static void writeUnsignedVarlong(long v, ByteBuffer buffer) {
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeDouble(double value, DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeDouble(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * Varint 是一种变长编码，用更少的字节表示小数字：
     *  每个字节的最高位（MSB）是** continuation bit**：1 表示还有后续字节，0 表示结束
     *  低7位存储实际数据
     * Varint 编码规则
     *  Varint 每 7 个有效位需要 1 个字节：
     *  1-7 位有效数据 → 1 字节
     *  8-14 位有效数据 → 2 字节
     *  15-21 位有效数据 → 3 字节
     *  以此类推...
     * 有效位数 = 32 - leadingZeros（对于非零值）
     *
     * 示例：
     *  值 1   → 0x01              (1字节)
     *  值 300 → 0xAC 0x02         (2字节)
     *  值 16384 → 0x80 0x80 0x01  (3字节)
     * 如何快速计算一个整数需要多少个字节来编码？
     *  传统方法（循环）
     *  int size = 1;
     *  while ((value & ~0x7F) != 0) {
     *     value >>>= 7;
     *     size++;
     *  }
     *  return size;
     *  数学公式法
     *  return (38 - leadingZeros) / 7 + leadingZeros / 32;
     * | value | binary | leadingZeros | 有效位数(32-leadingZeros) | 需要字节数 |
     * |-------|--------|--------------|--------------------------|-----------|
     * | 0 | `0000...0000` | 32 | 0 | 1 |
     * | 1 | `0000...0001` | 31 | 1 | 1 |
     * | 127 | `0000...1111111` | 25 | 7 | 1 |
     * | 128 | `0000...10000000` | 24 | 8 | 2 |
     * | 16383 | `0011...1111111` | 18 | 14 | 2 |
     * | 16384 | `0100...0000000` | 17 | 15 | 3 |
     * 公式推导：
     *  (38 - leadingZeros) / 7：计算需要多少个7位组
     *  leadingZeros / 32：处理 value=0 的特殊情况（此时前一项为 38/7=5，需要加 0 修正）
     * Java 编译器对除法的优化不够好，尤其是除以常数。作者使用了乘法替代除法的技巧。
     *  int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 0b10010010010010011) >>> 19;
     *  关键转换：x / 7 ≈ (x * M) >>> N
     *  其中 M = 0b10010010010010011 = 75091，这是怎么来的？
     *  M ≈ 2^N / 7，这里 N=19
     *  M = 2^19 / 7 = 524288 / 7 ≈ 74898.28
     *  取整后调整为 75091（通过测试验证精度）
     *  // 假设 38 - leadingZeros = 14（需要2个字节的情况）
     *  14 * 75091 = 1051274
     *  1051274 >>> 19 = 1051274 / 524288 = 2 ✓
     *
     *  // 假设 38 - leadingZeros = 21（需要3个字节的情况）
     *  21 * 75091 = 1576911
     *  1576911 >>> 19 = 1576911 / 524288 = 3 ✓
     *
     *
     * | 方法 | 操作次数 | 分支预测 |
     * |------|---------|---------|
     * | 循环法 | 平均 2-3 次迭代，每次有分支 | ❌ 可能失败 |
     * | 除法公式 | 2次除法 | ⚠️ 除法较慢 |
     * | **位运算优化** | **纯位运算+乘法** | ✅ **无分支，CPU流水线友好** |
     * 这段代码的核心思想：
     *  用 leading zeros 推算有效位数
     *  用乘法+移位替代除法（编译器优化技巧）
     *  处理边界情况（value=0）
     * 这是一种典型的高性能底层优化，在 Kafka 这种高吞吐系统中，避免分支预测失败和除法指令能带来显著的性能提升。
     *
     * @param value The signed value
     *
     * @see #writeUnsignedVarint(int, DataOutput)
     */
    public static int sizeOfUnsignedVarint(int value) {
        // Protocol buffers varint encoding is variable length, with a minimum of 1 byte
        // (for zero). The values themselves are not important. What's important here is
        // any leading zero bits are dropped from output.
        // We can use this leading zero(前导0) count w/ fast intrinsic(内在的) to calc(计算) the output length directly.
        // 我们可以利用这个前导零计数，结合快速内在函数，直接计算输出长度。

        // Test cases verify this matches the output for loop logic exactly.

        // return (38 - leadingZeros) / 7 + leadingZeros / 32;

        // The above formula provides the implementation, but the Java encoding is suboptimal
        // when we have a narrow range of integers, so we can do better manually

        int leadingZeros = Integer.numberOfLeadingZeros(value);
        int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 0b10010010010010011) >>> 19;
        /**
         * leadingZeros >>> 5 等价于 leadingZeros / 32
         *  当 value > 0 时，leadingZeros < 32，这部分为 0
         *  当 value = 0 时，leadingZeros = 32，32 >>> 5 = 1，修正结果为 1
         *
         * 以 value = 300 为例：
         *  // 300 的二进制: 0000 0000 0000 0000 0000 0001 0010 1100
         *  int leadingZeros = Integer.numberOfLeadingZeros(300); // = 23
         *
         * // 计算 (38 - 23) / 7 = 15 / 7 = 2
         * int temp = (38 - 23) * 75091; // = 15 * 75091 = 1126365
         * int leadingZerosBelow38DividedBy7 = 1126365 >>> 19; // = 2
         *
         * // 计算 23 / 32 = 0
         * int correction = 23 >>> 5; // = 0
         *
         * return 2 + 0; // = 2 ✓ (300 确实需要2字节编码)
         */
        return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     * @see #sizeOfUnsignedVarint(int)
     */
    public static int sizeOfVarlong(long value) {
        return sizeOfUnsignedVarlong((value << 1) ^ (value >> 63));
    }

    // visible for benchmarking
    public static int sizeOfUnsignedVarlong(long v) {
        // For implementation notes @see #sizeOfUnsignedVarint(int)
        // Similar logic is applied to allow for 64bit input -> 1-9byte output.
        // return (70 - leadingZeros) / 7 + leadingZeros / 64;

        int leadingZeros = Long.numberOfLeadingZeros(v);
        int leadingZerosBelow70DividedBy7 = ((70 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow70DividedBy7 + (leadingZeros >>> 6);
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
