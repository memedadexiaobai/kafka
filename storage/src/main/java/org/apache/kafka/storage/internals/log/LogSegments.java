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

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class encapsulates(概括，压缩) a thread-safe navigable map of LogSegment instances and provides the
 * required read and write behavior on the map.
 */
public class LogSegments {

    private final TopicPartition topicPartition;
    /* the segments of the log with key being LogSegment base offset and value being a LogSegment */
    /**
     * ConcurrentNavigableMap 是 Java 并发包 java.util.concurrent 中的一个接口，继承自 ConcurrentMap 和 NavigableMap。
     * 它结合了并发性和导航功能，提供了在多线程环境中高效、安全地操作有序映射的能力。
     * 优势
     *  1.并发性：ConcurrentNavigableMap 实现了 ConcurrentMap 接口，这意味着它在多线程环境下具有良好的并发性能。
     *      多个线程可以同时读取和写入不同的部分，而不会导致整个映射被锁住，从而提高了并发操作的效率。
     *  2.导航功能：继承自 NavigableMap，提供了丰富的导航方法，如 ceilingEntry、floorEntry、higherEntry 和 lowerEntry，这些方法允许你高效地查找最接近给定键的条目。
     *      这对于需要按顺序访问或查找特定范围内的条目的场景非常有用。
     *  3.线程安全：ConcurrentNavigableMap 的实现（如 ConcurrentSkipListMap）是线程安全的，无需外部同步即可在多线程环境中安全使用。
     * 适合的场景
     *  1.多线程环境下的有序映射：当你需要在一个多线程环境中维护一个有序的映射，并且希望在并发读写操作中保持高性能时，ConcurrentNavigableMap 是一个理想的选择。
     *  2.范围查询：如果你的应用需要频繁地进行范围查询（例如，查找某个键范围内的所有条目），ConcurrentNavigableMap 提供的导航方法可以显著提高查询效率。
     *  3.动态排序：在需要动态排序的场景中，ConcurrentNavigableMap 可以高效地插入和删除条目，同时保持映射的有序性。
     *  4.缓存和索引：在构建缓存或索引时，ConcurrentNavigableMap 可以提供高效的并发访问和有序性，适用于需要快速查找和更新的场景
     *
     * 常用方法
     *  put(K key, V value) 插入键值对。如果键已经存在，则替换旧值。
     *  get(Object key) 获取指定键的值。如果键不存在，返回 null。
     *  remove(Object key) 删除指定键的键值对。如果键不存在，不执行任何操作。
     *  containsKey(Object key) 检查映射是否包含指定的键。
     *  size() 返回映射中的键值对数量。
     *  isEmpty() 检查映射是否为空。
     *  firstKey() 和 lastKey() 分别返回映射中的第一个（最小）键和最后一个（最大）键。
     *  higherKey(K key) 和 lowerKey(K key) 分别返回大于和小于指定键的最小键和最大键。
     *  ceilingKey(K key) 和 floorKey(K key) 分别返回大于或等于和小于或等于指定键的最小键和最大键。
     *  subMap(K fromKey, K toKey) 返回一个子映射，包含从 fromKey（包含）到 toKey（不包含）之间的所有键值对。
     *  headMap(K toKey) 和 tailMap(K fromKey) 分别返回从映射的开始到 toKey（不包含）和从 fromKey（包含）到映射的结束的子映射。
     *  descendingMap() 返回一个逆序的视图，键值对按降序排列。
     *
     * 示例代码
     * public class ConcurrentNavigableMapExample {
     *     public static void main(String[] args) {
     *         ConcurrentNavigableMap<String, Integer> map = new ConcurrentSkipListMap<>();
     *
     *         map.put("apple", 1);
     *         map.put("banana", 2);
     *         map.put("cherry", 3);
     *
     *         System.out.println("Size: " + map.size()); // 输出: Size: 3
     *         System.out.println("First Key: " + map.firstKey()); // 输出: First Key: apple
     *         System.out.println("Last Key: " + map.lastKey()); // 输出: Last Key: cherry
     *
     *         System.out.println("Higher Key of 'banana': " + map.higherKey("banana")); // 输出: Higher Key of 'banana': cherry
     *         System.out.println("Lower Key of 'cherry': " + map.lowerKey("cherry")); // 输出: Lower Key of 'cherry': banana
     *
     *         System.out.println("Ceiling Key of 'banana': " + map.ceilingKey("banana")); // 输出: Ceiling Key of 'banana': banana
     *         System.out.println("Floor Key of 'banana': " + map.floorKey("banana")); // 输出: Floor Key of 'banana': banana
     *
     *         ConcurrentNavigableMap<String, Integer> subMap = map.subMap("apple", "cherry");
     *         System.out.println("Sub Map: " + subMap); // 输出: Sub Map: {apple=1, banana=2}
     *
     *         ConcurrentNavigableMap<String, Integer> headMap = map.headMap("banana");
     *         System.out.println("Head Map: " + headMap); // 输出: Head Map: {apple=1}
     *
     *         ConcurrentNavigableMap<String, Integer> tailMap = map.tailMap("banana");
     *         System.out.println("Tail Map: " + tailMap); // 输出: Tail Map: {banana=2, cherry=3}
     *
     *         ConcurrentNavigableMap<String, Integer> descendingMap = map.descendingMap();
     *         System.out.println("Descending Map: " + descendingMap); // 输出: Descending Map: {cherry=3, banana=2, apple=1}
     *     }
     * }
     */
    private final ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    /**
     * Create new instance.
     *
     * @param topicPartition the TopicPartition associated with the segments
     *                        (useful for logging purposes)
     */
    public LogSegments(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * Return true if the segments are empty, false otherwise.
     *
     * This method is thread-safe.
     */
    public boolean isEmpty() {
        return segments.isEmpty();
    }

    /**
     * Return true if the segments are non-empty, false otherwise.
     *
     * This method is thread-safe.
     */
    public boolean nonEmpty() {
        return !isEmpty();
    }

    /**
     * Add the given segment, or replace an existing entry.
     *
     * This method is thread-safe.
     *
     * @param segment the segment to add
     */
    public LogSegment add(LogSegment segment) {
        return this.segments.put(segment.baseOffset(), segment);
    }

    /**
     * Remove the segment at the provided offset.
     *
     * This method is thread-safe.
     *
     * @param offset the offset to be removed
     */
    public void remove(long offset) {
        segments.remove(offset);
    }

    /**
     * Clears all entries.
     *
     * This method is thread-safe.
     */
    public void clear() {
        segments.clear();
    }

    /**
     * Close all segments.
     */
    public void close() throws IOException {
        for (LogSegment s : values())
            s.close();
    }

    /**
     * Close the handlers for all segments.
     */
    public void closeHandlers() {
        for (LogSegment s : values())
            s.closeHandlers();
    }

    /**
     * Update the directory reference for the log and indices of all segments.
     *
     * @param dir the renamed directory
     */
    public void updateParentDir(File dir) {
        for (LogSegment s : values())
            s.updateParentDir(dir);
    }

    /**
     * Take care! this is an O(n) operation, where n is the number of segments.
     *
     * This method is thread-safe.
     *
     * @return The number of segments.
     *
     */
    public int numberOfSegments() {
        return segments.size();
    }

    /**
     * @return the base offsets of all segments
     */
    public Collection<Long> baseOffsets() {
        return values().stream().map(LogSegment::baseOffset).collect(Collectors.toList());
    }

    /**
     * Return true if a segment exists at the provided offset, false otherwise.
     *
     * This method is thread-safe.
     *
     * @param offset the segment to be checked
     */
    public boolean contains(long offset) {
        return segments.containsKey(offset);
    }

    /**
     * Retrieves a segment at the specified offset.
     *
     * This method is thread-safe.
     *
     * @param offset the segment to be retrieved
     *
     * @return the segment if it exists, otherwise Empty.
     */
    public Optional<LogSegment> get(long offset) {
        return Optional.ofNullable(segments.get(offset));
    }

    /**
     * @return an iterator to the log segments ordered from oldest to newest.
     */
    public Collection<LogSegment> values() {
        return segments.values();
    }

    /**
     * @return An iterator to all segments
     * beginning with the segment that includes "from"
     * and ending with the segment that includes up to(达到，接近于) "to-1" or the end of the log (if to > end of log).
     */
    public Collection<LogSegment> values(long from, long to) {
        if (from == to) {
            // Handle non-segment-aligned empty sets
            return Collections.emptyList();
        } else if (to < from) {
            throw new IllegalArgumentException("Invalid log segment range: requested segments in " + topicPartition +
                    " from offset " + from + " which is greater than limit offset " + to);
        } else {
            // 小于或者等于 from 的最大值
            Long floor = segments.floorKey(from);
            if (floor != null)
                return segments.subMap(floor, to).values();
            return segments.headMap(to).values();
        }
    }

    public Collection<LogSegment> nonActiveLogSegmentsFrom(long from) {
        LogSegment activeSegment = lastSegment().get();
        if (from > activeSegment.baseOffset())
            return Collections.emptyList();
        else
            return values(from, activeSegment.baseOffset());
    }

    /**
     * Return the entry associated with the greatest offset less than or equal to the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    private Optional<Map.Entry<Long, LogSegment>> floorEntry(long offset) {
        return Optional.ofNullable(segments.floorEntry(offset));
    }

    /**
     * Return the log segment with the greatest offset less than or equal to the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> floorSegment(long offset) {
        return floorEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the greatest offset strictly less than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    private Optional<Map.Entry<Long, LogSegment>> lowerEntry(long offset) {
        return Optional.ofNullable(segments.lowerEntry(offset));
    }

    /**
     * Return the log segment with the greatest offset strictly(严格的，只) less than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> lowerSegment(long offset) {
        return lowerEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the smallest offset strictly greater than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> higherEntry(long offset) {
        return Optional.ofNullable(segments.higherEntry(offset));
    }

    /**
     * Return the log segment with the smallest offset strictly greater than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> higherSegment(long offset) {
        return higherEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the smallest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> firstEntry() {
        return Optional.ofNullable(segments.firstEntry());
    }

    /**
     * Return the log segment associated with the smallest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> firstSegment() {
        return firstEntry().map(Map.Entry::getValue);
    }

    /**
     * @return the base offset of the log segment associated with the smallest offset, if it exists
     */
    public OptionalLong firstSegmentBaseOffset() {
        return firstSegment().map(logSegment -> OptionalLong.of(logSegment.baseOffset()))
                .orElseGet(OptionalLong::empty);
    }

    /**
     * Return the entry associated with the greatest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> lastEntry() {
        return Optional.ofNullable(segments.lastEntry());
    }

    /**
     * Return the log segment with the greatest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> lastSegment() {
        return lastEntry().map(Map.Entry::getValue);
    }

    /**
     * @return an iterable with log segments ordered from lowest base offset to highest,
     *         each segment returned has a base offset strictly greater than the provided baseOffset.
     */
    public Collection<LogSegment> higherSegments(long baseOffset) {
        Long higherOffset = segments.higherKey(baseOffset);
        if (higherOffset != null)
            return segments.tailMap(higherOffset, true).values();
        return Collections.emptyList();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return lastSegment().get();
    }

    public long sizeInBytes() {
        return LogSegments.sizeInBytes(values());
    }

    /**
     * Returns an Iterable containing segments matching the provided predicate.
     *
     * @param predicate the predicate to be used for filtering segments.
     */
    public Collection<LogSegment> filter(Predicate<LogSegment> predicate) {
        return values().stream().filter(predicate).collect(Collectors.toList());
    }

    /**
     * Calculate a log's size (in bytes) from the provided log segments.
     *
     * @param segments The log segments to calculate the size of
     * @return Sum of the log segments' sizes (in bytes)
     */
    public static long sizeInBytes(Collection<LogSegment> segments) {
        return segments.stream().mapToLong(LogSegment::size).sum();
    }
}
