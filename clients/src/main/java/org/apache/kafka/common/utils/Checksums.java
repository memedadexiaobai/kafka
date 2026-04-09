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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * Utility methods for `Checksum` instances.
 *
 * Implementation note: we can add methods to our implementations of CRC32 and CRC32C, but we cannot do the same for
 * the Java implementations (we prefer the Java 9 implementation of CRC32C if available). A utility class is the
 * simplest way to add methods that are useful for all Checksum implementations.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public final class Checksums {
    private static final MethodHandle BYTE_BUFFER_UPDATE;

    static {
        MethodHandle byteBufferUpdate = null;
        if (Java.IS_JAVA9_COMPATIBLE) {
            try {
                byteBufferUpdate = MethodHandles.publicLookup().findVirtual(Checksum.class, "update",
                    MethodType.methodType(void.class, ByteBuffer.class));
            } catch (Throwable t) {
                handleUpdateThrowable(t);
            }
        }
        BYTE_BUFFER_UPDATE = byteBufferUpdate;
    }

    private Checksums() {
    }

    /**
     * Uses {@link Checksum#update} on {@code buffer}'s content, without modifying its position and limit.<br>
     * This is semantically equivalent to {@link #update(Checksum, ByteBuffer, int, int)} with {@code offset = 0}.
     */
    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    /**
     * Uses {@link Checksum#update} on {@code buffer}'s content, starting from the given {@code offset}
     * by the provided {@code length}, without modifying its position and limit.
     */
    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else if (BYTE_BUFFER_UPDATE != null && buffer.isDirect()) {
            final int oldPosition = buffer.position();
            final int oldLimit = buffer.limit();
            try {
                // save a slice to be used to save an allocation in the hot-path
                final int start = oldPosition + offset;
                buffer.limit(start + length);
                buffer.position(start);
                BYTE_BUFFER_UPDATE.invokeExact(checksum, buffer);
            } catch (Throwable t) {
                handleUpdateThrowable(t);
            } finally {
                // reset buffer's offsets
                buffer.limit(oldLimit);
                buffer.position(oldPosition);
            }
        } else {
            // slow-path
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++) {
                checksum.update(buffer.get(i));
            }
        }
    }

    private static void handleUpdateThrowable(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        throw new IllegalStateException(t);
    }
    
    public static void updateInt(Checksum checksum, int input) {
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }

    /**
     * 将 long 类型（8字节）逐字节分解并更新到校验和（Checksum）中。
     * 核心目的
     *  将一个 64 位的 long 值按大端序（Big-Endian）拆分成 8 个字节，逐个喂给 Checksum 算法。
     * 为什么要逐字节？
     *  Checksum API 的限制
     *  Java 的 Checksum 接口（如 CRC32）主要提供这些方法：
     *  void update(int b);      // 更新1个字节
     *  void update(byte[] b, int off, int len);  // 更新字节数组
     * 问题：没有直接接受 long 的方法！
     * 所以需要手动将 long 拆解为字节
     * | 对比维度 | 手动移位 | ByteBuffer | Unsafe |
     * |---------|---------|------------|--------|
     * | **性能** | ✅ 最快（无对象分配） | ❌ 需分配数组 | ✅ 快但危险 |
     * | **GC 压力** | ✅ 零分配 | ❌ 每次分配8字节数组 | ✅ 零分配 |
     * | **可读性** | ⚠️ 位运算晦涩 | ✅ 语义清晰 | ❌ 不安全 |
     * | **JIT 优化** | ✅ 易优化 | ⚠️ 依赖实现 | ✅ 但非标准 |
     *
     * **关键优势**：
     * 1. **零内存分配**：不需要创建临时数组或 ByteBuffer 对象
     * 2. **CPU 友好**：移位和类型转换都是极快的 CPU 指令
     * 3. **确定性**：不依赖 ByteBuffer 的字节序设置
     * 4. **高频调用场景**：Kafka 每条消息都要计算 CRC，性能至关重要
     *
     * 大端序 vs 小端序
     *  为什么用大端序（先传高字节）？
     *  网络协议标准：TCP/IP、Kafka 协议都用大端序
     *  一致性：确保不同平台（x86/ARM）计算的 CRC 相同
     *  人类可读：0x01020304 按 01 02 03 04 顺序更直观
     *  如果用小端序，代码会反过来：
     *
     * 这样写的原因：
     *  ✅ API 限制：Checksum 只能接受字节，不能直接处理 long
     *  ✅ 性能最优：零内存分配，纯 CPU 运算
     *  ✅ 协议一致：大端序符合网络传输标准
     *  ✅ 高频优化：Kafka 每条消息都要算 CRC，必须极致优化
     * 这是一种典型的底层性能优化手法，在高性能网络协议和序列化场景中非常常见。虽然代码看起来冗长，但执行效率远超其他方案。
     */
    public static void updateLong(Checksum checksum, long input) {
        // 第1字节：最高有效字节 (MSB)
        checksum.update((byte) (input >> 56));
        checksum.update((byte) (input >> 48));
        checksum.update((byte) (input >> 40));
        checksum.update((byte) (input >> 32));
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        // 第8字节：最低有效字节 (LSB)
        checksum.update((byte) input /* >> 0 */);
    }
}
