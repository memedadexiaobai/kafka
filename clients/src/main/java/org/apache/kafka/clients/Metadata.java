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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public class Metadata implements Closeable {
    private final Logger log;
    private final ExponentialBackoff refreshBackoff;// 指数退避算法
    private final long metadataExpireMs;// 元数据过期时间
    private int updateVersion;  // bumped(碰撞) on every metadata response
    private int requestVersion; // bumped on every new topic addition
    private long lastRefreshMs; // 上次尝试刷新的时间
    private long lastSuccessfulRefreshMs; // 上次成功刷新的时间
    private long attempts;// 连续失败次数
    private KafkaException fatalException;
    private Set<String> invalidTopics;
    private Set<String> unauthorizedTopics;
    private volatile MetadataSnapshot metadataSnapshot = MetadataSnapshot.empty();
    private boolean needFullUpdate;
    private boolean needPartialUpdate;
    private long equivalentResponseCount;// 连续等效响应次数
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;
    /** Addresses with which the metadata was originally bootstrapped. */
    private List<InetSocketAddress> bootstrapAddresses;

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param refreshBackoffMaxMs      The maximum amount of time to wait between metadata refreshes
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param logContext               Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long refreshBackoffMaxMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoff = new ExponentialBackoff(
            refreshBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            refreshBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.attempts = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needFullUpdate = false;
        this.needPartialUpdate = false;
        this.equivalentResponseCount = 0;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
        this.invalidTopics = Collections.emptySet();
        this.unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Get the current cluster info without blocking
     */
    public Cluster fetch() {
        return metadataSnapshot.cluster();
    }

    /**
     * Get the current metadata cache.
     */
    public MetadataSnapshot fetchMetadataSnapshot() {
        return metadataSnapshot;
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     * There are two calculations for backing off based on how many attempts to retrieve metadata have been made
     * since the last successful response, and how many equivalent metadata responses have been received.
     * The second of these allows backing off when there are errors to do with stale metadata, even though the
     * metadata responses are clean.
     * <p>
     * This can be used to check whether it's worth requesting an update in the knowledge that it will
     * not be delayed if this method returns 0.
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again 距离下次允许更新的毫秒数（返回 0 表示可以立即更新）
     *
     * 计算距离下次允许更新元数据还需要等待多长时间，核心目的是防止元数据刷新过于频繁，减少对 broker 的压力
     *
     * 核心逻辑：两种退避机制
     *  该方法计算两个退避时间，取较大值:返回 0 表示可以立即更新，否则调用者需要等待返回的时间后再重试
     *  两种退避的区别
     *   ┌────────────┬───────────────────────────────────┬─────────────────────────┬───────────────────────────────────────┐
     *   │    场景     │             触发条件              │         计数器            │               何时重置                │
     *   ├────────────┼───────────────────────────────────┼─────────────────────────┼───────────────────────────────────────┤
     *   │ 失败退避    │ broker 不可用、网络错误等            │ attempts                │ 成功更新时 (update() 中 attempts = 0) │
     *   ├────────────┼───────────────────────────────────┼─────────────────────────┼───────────────────────────────────────┤
     *   │ 无进展退避   │ leader epoch 无变化、响应内容相同    │ equivalentResponseCount │ leader epoch 变化 或 元数据过期       │
     *   └────────────┴───────────────────────────────────┴─────────────────────────┴───────────────────────────────────────┘
     *
     * 示例流程:
     *   时刻 T0: 请求元数据 → 失败 → attempts=1, lastRefreshMs=T0
     *   时刻 T1: 调用 timeToAllowUpdate(now=T1) → 返回 backoffForAttempts (如 100ms)
     *   时刻 T2(>T0+100ms): 再次请求 → 成功 → attempts=0, lastSuccessfulRefreshMs=T2
     *   时刻 T3: 再次请求 → leader epoch 未变 → equivalentResponseCount=1
     *   时刻 T4: 调用 timeToAllowUpdate → 可能返回 backoffForEquivalentResponseCount
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        // Calculate the backoff for attempts which acts when metadata responses fail
        // 失败重试退避，当元数据请求失败时会触发（attempts 计数）：
        //   attempts：记录连续失败的次数（在 failedUpdate() 方法中递增）
        //   refreshBackoff：使用指数退避算法，失败次数越多，退避时间越长
        long backoffForAttempts = Math.max(this.lastRefreshMs +
                this.refreshBackoff.backoff(this.attempts > 0 ? this.attempts - 1 : 0) - nowMs, 0);

        // Periodic updates based on expiration resets the equivalent(相同的) response count so exponential backoff is not used
        //  基于过期时间的定期更新会重置等效响应计数，因此不使用指数退避算法
        if (Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0) == 0) {// 大于0说明没到元数据过期时间， 0的时候说明元数据过期了
            this.equivalentResponseCount = 0; //记录连续收到"等效响应"的次数 元数据过期时重置此计数
        }

        // Calculate the backoff for equivalent responses which acts(生效) when metadata responses are not making progress
        // 计算等效响应的退避时间，该退避在元数据响应未取得进展时生效
        // backoffForEquivalentResponseCount - 无进展退避,当元数据响应无实质进展时触发
        long backoffForEquivalentResponseCount = Math.max(this.lastRefreshMs +
                (this.equivalentResponseCount > 0 ? this.refreshBackoff.backoff(this.equivalentResponseCount - 1) : 0) - nowMs, 0);

        return Math.max(backoffForAttempts, backoffForEquivalentResponseCount);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed). If an update has been requested, the metadata
     * expiry time is now.
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info 距离下次更新还需要等待的毫秒数（返回 0 表示应该立即更新）
     *
     * 计算距离下次元数据更新的时间，综合了三个因素：过期时间、退避时间、是否显式请求更新
     * 结合了"过期时间"和"退避时间"两个维度，确保元数据更新既不会过于频繁（退避），也不会过于陈旧（过期）
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        /**
         * - 如果 updateRequested() 返回 true（即 needFullUpdate 或 needPartialUpdate 为 true）→ timeToExpire = 0
         *     - 表示有显式更新请求，忽略过期时间，可以立即更新
         * - 否则 → 计算距离元数据过期还有多久
         *     - lastSuccessfulRefreshMs + metadataExpireMs - nowMs
         *     - 如果已经是负数（已过期），用 Math.max(..., 0) 修正为 0
         */
        long timeToExpire = updateRequested() ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        // timeToExpire:元数据过期时间
        // timeToAllowUpdate:获取基于退避策略的最小等待时间
        // 同时满足过期时间和退避时间的要求，取较晚的那个作为下次更新时间。
        /**
         * 三种更新触发条件
         *   ┌────────────────┬──────────────────────────────────────────────────┬──────────────┬───────────────────┬──────────┐
         *   │      条件       │                     计算方式                      │ timeToExpire │ timeToAllowUpdate │   结果   │
         *   ├────────────────┼──────────────────────────────────────────────────┼──────────────┼───────────────────┼──────────┤
         *   │ 显式请求更新     │ updateRequested()=true                           │ 0            │ 退避时间           │ 退避时间 │
         *   ├────────────────┼──────────────────────────────────────────────────┼──────────────┼───────────────────┼──────────┤
         *   │ 元数据已过期     │ now > lastSuccessfulRefreshMs + metadataExpireMs │ 0            │ 退避时间           │ 退避时间 │
         *   ├────────────────┼──────────────────────────────────────────────────┼──────────────┼───────────────────┼──────────┤
         *   │ 未过期且无请求   │ 正常状态                                           │ 剩余过期时间   │ 退避时间           │ 取较大值 │
         *   └────────────────┴──────────────────────────────────────────────────┴──────────────┴───────────────────┴──────────┘
         *
         * 与 timeToAllowUpdate 的关系
         *
         *   timeToNextUpdate()
         *       ├── timeToExpire（过期维度）
         *       │   └── 受 metadataExpireMs 和显式请求影响
         *       │
         *       └── timeToAllowUpdate（退避维度）
         *           ├── backoffForAttempts（失败重试）
         *           └── backoffForEquivalentResponseCount（无进展）
         *
         * 示例场景
         *   场景 1：正常情况（未过期，无请求）
         *   lastSuccessfulRefreshMs = 1000
         *   metadataExpireMs = 30000
         *   nowMs = 5000
         *
         *   timeToExpire = 1000 + 30000 - 5000 = 26000
         *   timeToAllowUpdate = 0 (没有失败或等效响应)
         *
         *   返回: max(26000, 0) = 26000  // 还要等26秒
         *
         *   场景 2：元数据已过期
         *   lastSuccessfulRefreshMs = 1000
         *   metadataExpireMs = 30000
         *   nowMs = 40000
         *
         *   timeToExpire = max(1000 + 30000 - 40000, 0) = 0  // 已过期
         *   timeToAllowUpdate = 0
         *
         *   返回: max(0, 0) = 0  // 立即更新
         *
         *   场景 3：显式请求更新
         *   needFullUpdate = true  // 比如新加入一个 topic
         *   nowMs = 5000
         *
         *   timeToExpire = 0  // 被强制设为0
         *   timeToAllowUpdate = 100  // 退避100ms
         *
         *   返回: max(0, 100) = 100  // 只受退避时间限制
         *
         *   场景 4：多次失败后
         *   attempts = 3
         *   lastRefreshMs = 4000
         *   nowMs = 5000
         *
         *   timeToExpire = 26000 (假设未过期)
         *   timeToAllowUpdate = backoff(2) ≈ 200ms (指数退避)
         *
         *   返回: max(26000, 200) = 26000  // 还是受过期时间限制
         *
         *   场景 5：元数据过期 + 多次失败
         *   attempts = 3
         *   lastRefreshMs = 4000
         *   nowMs = 40000 (元数据已过期)
         *
         *   timeToExpire = 0
         *   timeToAllowUpdate = 200
         *
         *   返回: max(0, 200) = 200  // 受退避时间限制
         */
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * Request an update of the current cluster metadata info, permitting backoff based on the number of
     * equivalent metadata responses, which indicates that responses did not make progress and may be stale.
     * 
     * @param resetEquivalentResponseBackoff Whether to reset backing off based on consecutive equivalent responses.
     *                                       This should be set to <i>false</i> in situations where the update is
     *                                       being requested to retry an operation, such as when the leader has
     *                                       changed. It should be set to <i>true</i> in situations where new
     *                                       metadata is being requested, such as adding a topic to a subscription.
     *                                       In situations where it's not clear, it's best to use <i>true</i>.
     * 
     * @return The current updateVersion before the update
     */
    public synchronized int requestUpdate(final boolean resetEquivalentResponseBackoff) {
        this.needFullUpdate = true;
        if (resetEquivalentResponseBackoff) {
            this.equivalentResponseCount = 0;
        }
        return this.updateVersion;
    }

    /**
     * Request an immediate update of the current cluster metadata info, because the caller is interested in
     * metadata that is being newly requested.
     * @return The current updateVersion before the update
     */
    public synchronized int requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.needPartialUpdate = true;
        this.equivalentResponseCount = 0;
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * Request an update for the partition metadata iff we have seen a newer leader epoch. This is called by the client
     * any time it handles a response from the broker that includes leader epoch, except for update via Metadata RPC which
     * follows a different code path ({@link #update}).
     *
     * @param topicPartition
     * @param leaderEpoch
     * @return true if we updated the last seen epoch, false otherwise
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (leaderEpoch < 0)
            throw new IllegalArgumentException("Invalid leader epoch " + leaderEpoch + " (must be non-negative)");

        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);

        final boolean updated;
        if (oldEpoch == null) {
            log.debug("Not replacing null epoch with new epoch {} for partition {}", leaderEpoch, topicPartition);
            updated = false;
        } else if (leaderEpoch > oldEpoch) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, leaderEpoch);
            updated = true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            updated = false;
        }

        this.needFullUpdate = this.needFullUpdate || updated;
        return updated;
    }

    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needFullUpdate || this.needPartialUpdate;
    }

    public synchronized void addClusterUpdateListener(ClusterResourceListener listener) {
        this.clusterResourceListeners.maybeAdd(listener);
    }

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    synchronized Optional<MetadataResponse.PartitionMetadata> partitionMetadataIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        Optional<MetadataResponse.PartitionMetadata> partitionMetadata = metadataSnapshot.partitionMetadata(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return partitionMetadata;
        } else {
            return partitionMetadata.filter(metadata ->
                    metadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH).equals(epoch));
        }
    }

    /**
     * @return a mapping from topic names to topic IDs for all topics with valid IDs in the cache
     */
    public Map<String, Uuid> topicIds() {
        return metadataSnapshot.topicIds();
    }

    public synchronized LeaderAndEpoch currentLeader(TopicPartition topicPartition) {
        Optional<MetadataResponse.PartitionMetadata> maybeMetadata = partitionMetadataIfCurrent(topicPartition);
        if (!maybeMetadata.isPresent())
            return new LeaderAndEpoch(Optional.empty(), Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition)));

        MetadataResponse.PartitionMetadata partitionMetadata = maybeMetadata.get();
        Optional<Integer> leaderEpochOpt = partitionMetadata.leaderEpoch;
        Optional<Node> leaderNodeOpt = partitionMetadata.leaderId.flatMap(metadataSnapshot::nodeById);
        return new LeaderAndEpoch(leaderNodeOpt, leaderEpochOpt);
    }

    public synchronized void bootstrap(List<InetSocketAddress> addresses) {
        this.needFullUpdate = true;
        this.updateVersion += 1;
        this.metadataSnapshot = MetadataSnapshot.bootstrap(addresses);
        this.bootstrapAddresses = addresses;
    }

    public synchronized void rebootstrap() {
        log.info("Rebootstrapping with {}", this.bootstrapAddresses);
        this.bootstrap(this.bootstrapAddresses);
    }

    /**
     * Update metadata assuming the current request version.
     *
     * For testing only.
     */
    public synchronized void updateWithCurrentRequestVersion(MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        this.update(this.requestVersion, response, isPartialUpdate, nowMs);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion(long)}.
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        this.needPartialUpdate = requestVersion < this.requestVersion;
        this.lastRefreshMs = nowMs;
        this.attempts = 0;
        this.updateVersion += 1;
        if (!isPartialUpdate) {
            this.needFullUpdate = false;
            this.lastSuccessfulRefreshMs = nowMs;
        }
        // If we subsequently find that the metadata response is not equivalent to the metadata already known,
        // this count is reset to 0 in updateLatestMetadata()
        this.equivalentResponseCount++;

        String previousClusterId = metadataSnapshot.clusterResource().clusterId();

        this.metadataSnapshot = handleMetadataResponse(response, isPartialUpdate, nowMs);

        Cluster cluster = metadataSnapshot.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, nowMs));

        String newClusterId = metadataSnapshot.clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        clusterResourceListeners.onUpdate(metadataSnapshot.clusterResource());

        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.metadataSnapshot);
    }

    /**
     * Updates the partition-leadership info in the metadata. Update is done by merging existing metadata with the input leader information and nodes.
     * This is called whenever partition-leadership updates are returned in a response from broker(ex - ProduceResponse & FetchResponse).
     * Note that the updates via Metadata RPC are handled separately in ({@link #update}).
     * Both partitionLeader and leaderNodes override the existing metadata. Non-overlapping metadata is kept as it is.
     * @param partitionLeaders map of new leadership information for partitions.
     * @param leaderNodes a list of nodes for leaders in the above map.
     * @return a set of partitions, for which leaders were updated.
     */
    public synchronized Set<TopicPartition> updatePartitionLeadership(Map<TopicPartition, LeaderIdAndEpoch> partitionLeaders, List<Node> leaderNodes) {
        Map<Integer, Node> newNodes = leaderNodes.stream().collect(Collectors.toMap(Node::id, node -> node));
        // Insert non-overlapping nodes from existing-nodes into new-nodes.
        this.metadataSnapshot.cluster().nodes().stream().forEach(node -> newNodes.putIfAbsent(node.id(), node));

        // Create partition-metadata for all updated partitions. Exclude updates for partitions -
        // 1. for which the corresponding partition has newer leader in existing metadata.
        // 2. for which corresponding leader's node is missing in the new-nodes.
        // 3. for which the existing metadata doesn't know about the partition.
        List<PartitionMetadata> updatePartitionMetadata = new ArrayList<>();
        for (Entry<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeader: partitionLeaders.entrySet()) {
            TopicPartition partition = partitionLeader.getKey();
            Metadata.LeaderAndEpoch currentLeader = currentLeader(partition);
            Metadata.LeaderIdAndEpoch newLeader = partitionLeader.getValue();
            if (!newLeader.epoch.isPresent() || !newLeader.leaderId.isPresent()) {
                log.debug("For {}, incoming leader information is incomplete {}", partition, newLeader);
                continue;
            }
            if (currentLeader.epoch.isPresent() && newLeader.epoch.get() <= currentLeader.epoch.get()) {
                log.debug("For {}, incoming leader({}) is not-newer than the one in the existing metadata {}, so ignoring.", partition, newLeader, currentLeader);
                continue;
            }
            if (!newNodes.containsKey(newLeader.leaderId.get())) {
                log.debug("For {}, incoming leader({}), the corresponding node information for node-id {} is missing, so ignoring.", partition, newLeader, newLeader.leaderId.get());
                continue;
            }
            if (!this.metadataSnapshot.partitionMetadata(partition).isPresent()) {
                log.debug("For {}, incoming leader({}), partition metadata is no longer cached, ignoring.", partition, newLeader);
                continue;
            }

            MetadataResponse.PartitionMetadata existingMetadata = this.metadataSnapshot.partitionMetadata(partition).get();
            MetadataResponse.PartitionMetadata updatedMetadata = new MetadataResponse.PartitionMetadata(
                existingMetadata.error,
                partition,
                newLeader.leaderId,
                newLeader.epoch,
                existingMetadata.replicaIds,
                existingMetadata.inSyncReplicaIds,
                existingMetadata.offlineReplicaIds
            );
            updatePartitionMetadata.add(updatedMetadata);

            lastSeenLeaderEpochs.put(partition, newLeader.epoch.get());
        }

        if (updatePartitionMetadata.isEmpty()) {
            log.debug("No relevant metadata updates.");
            return new HashSet<>();
        }

        Set<String> updatedTopics = updatePartitionMetadata.stream().map(MetadataResponse.PartitionMetadata::topic).collect(Collectors.toSet());

        // Get topic-ids for updated topics from existing topic-ids.
        Map<String, Uuid> existingTopicIds = this.metadataSnapshot.topicIds();
        Map<String, Uuid> topicIdsForUpdatedTopics = updatedTopics.stream()
            .filter(existingTopicIds::containsKey)
            .collect(Collectors.toMap(e -> e, existingTopicIds::get));

        if (log.isDebugEnabled()) {
            updatePartitionMetadata.forEach(
                partMetadata -> log.debug("For {} updating leader information, updated metadata is {}.", partMetadata.topicPartition, partMetadata)
            );
        }

        // Fetch responses can include partition level leader changes, when this happens, we perform a partial
        // metadata update, by keeping the unchanged partition and update the changed partitions.
        this.metadataSnapshot = metadataSnapshot.mergeWith(
            metadataSnapshot.clusterResource().clusterId(),
            newNodes,
            updatePartitionMetadata,
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
            metadataSnapshot.cluster().controller(),
            topicIdsForUpdatedTopics,
            (topic, isInternal) -> true);
        clusterResourceListeners.onUpdate(metadataSnapshot.clusterResource());

        return updatePartitionMetadata.stream()
            .map(metadata -> metadata.topicPartition)
            .collect(Collectors.toSet());
    }

    private void maybeSetMetadataError(Cluster cluster) {
        clearRecoverableErrors();
        checkInvalidTopics(cluster);
        checkUnauthorizedTopics(cluster);
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            invalidTopics = new HashSet<>(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            unauthorizedTopics = new HashSet<>(cluster.unauthorizedTopics());
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private MetadataSnapshot handleMetadataResponse(MetadataResponse metadataResponse, boolean isPartialUpdate, long nowMs) {
        // All encountered topics.
        Set<String> topics = new HashSet<>();

        // Retained topics to be passed to the metadata cache.
        Set<String> internalTopics = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();

        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<String, Uuid> oldTopicIds = metadataSnapshot.topicIds();
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            String topicName = metadata.topic();
            Uuid topicId = metadata.topicId();
            topics.add(topicName);
            // We can only reason about topic ID changes when both IDs are valid, so keep oldId null unless the new metadata contains a topic ID
            Uuid oldTopicId = null;
            if (!Uuid.ZERO_UUID.equals(topicId)) {
                topicIds.put(topicName, topicId);
                oldTopicId = oldTopicIds.get(topicName);
            } else {
                topicId = null;
            }

            if (!retainTopic(topicName, metadata.isInternal(), nowMs))
                continue;

            if (metadata.isInternal())
                internalTopics.add(topicName);

            if (metadata.error() == Errors.NONE) {
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    // Even if the partition's metadata includes an error, we need to handle
                    // the update to catch new epochs
                    updateLatestMetadata(partitionMetadata, metadataResponse.hasReliableLeaderEpochs(), topicId, oldTopicId)
                        .ifPresent(partitions::add);

                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                partitionMetadata.topicPartition, partitionMetadata.error);
                        requestUpdate(false);
                    }
                }
            } else {
                if (metadata.error().exception() instanceof InvalidMetadataException) {
                    log.debug("Requesting metadata update for topic {} due to error {}", topicName, metadata.error());
                    requestUpdate(false);
                }

                if (metadata.error() == Errors.INVALID_TOPIC_EXCEPTION)
                    invalidTopics.add(topicName);
                else if (metadata.error() == Errors.TOPIC_AUTHORIZATION_FAILED)
                    unauthorizedTopics.add(topicName);
            }
        }

        Map<Integer, Node> nodes = metadataResponse.brokersById();
        if (isPartialUpdate)
            return this.metadataSnapshot.mergeWith(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(), topicIds,
                (topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs));
        else
            return new MetadataSnapshot(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(), topicIds);
    }

    /**
     * Compute the latest partition metadata to cache given ordering by leader epochs (if both
     * available and reliable) and whether the topic ID changed.
     */
    private Optional<MetadataResponse.PartitionMetadata> updateLatestMetadata(
            MetadataResponse.PartitionMetadata partitionMetadata,
            boolean hasReliableLeaderEpoch,
            Uuid topicId,
            Uuid oldTopicId) {
        TopicPartition tp = partitionMetadata.topicPartition;
        if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch.isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch.get();
            Integer currentEpoch = lastSeenLeaderEpochs.get(tp);
            if (currentEpoch == null) {
                // We have no previous info, so we can just insert the new epoch info
                log.debug("Setting the last seen epoch of partition {} to {} since the last known epoch was undefined.",
                        tp, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                this.equivalentResponseCount = 0;
                return Optional.of(partitionMetadata);
            } else if (topicId != null && !topicId.equals(oldTopicId)) {
                // If the new topic ID is valid and different from the last seen topic ID, update the metadata.
                // Between the time that a topic is deleted and re-created, the client may lose track of the
                // corresponding topicId (i.e. `oldTopicId` will be null). In this case, when we discover the new
                // topicId, we allow the corresponding leader epoch to override the last seen value.
                log.info("Resetting the last seen epoch of partition {} to {} since the associated topicId changed from {} to {}",
                        tp, newEpoch, oldTopicId, topicId);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                this.equivalentResponseCount = 0;
                return Optional.of(partitionMetadata);
            } else if (newEpoch >= currentEpoch) {
                // If the received leader epoch is at least the same as the previous one, update the metadata
                log.debug("Updating last seen epoch for partition {} from {} to epoch {} from new metadata", tp, currentEpoch, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                if (newEpoch > currentEpoch) {
                    this.equivalentResponseCount = 0;
                }
                return Optional.of(partitionMetadata);
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                log.debug("Got metadata for an older epoch {} (current is {}) for partition {}, not updating", newEpoch, currentEpoch, tp);
                return metadataSnapshot.partitionMetadata(tp);
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp);
            this.equivalentResponseCount = 0;
            return Optional.of(partitionMetadata.withoutLeaderEpoch());
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, clear and throw the exception.
     * This is used by the consumer to propagate any fatal exceptions or topic exceptions for any of the topics
     * in the consumer's Metadata.
     */
    public synchronized void maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException(this::recoverableException);
    }

    /**
     * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
     * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
     * in the last metadata update.
     */
    protected synchronized void maybeThrowFatalException() {
        KafkaException metadataException = this.fatalException;
        if (metadataException != null) {
            fatalException = null;
            throw metadataException;
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, throw exception if the exception
     * is fatal or related to the specified topic. All exceptions from the last metadata update are cleared.
     * This is used by the producer to propagate topic metadata errors for send requests.
     */
    public synchronized void maybeThrowExceptionForTopic(String topic) {
        clearErrorsAndMaybeThrowException(() -> recoverableExceptionForTopic(topic));
    }

    private void clearErrorsAndMaybeThrowException(Supplier<KafkaException> recoverableExceptionSupplier) {
        KafkaException metadataException = Optional.ofNullable(fatalException).orElseGet(recoverableExceptionSupplier);
        fatalException = null;
        clearRecoverableErrors();
        if (metadataException != null)
            throw metadataException;
    }

    // We may be able to recover from this exception if metadata for this topic is no longer needed
    private KafkaException recoverableException() {
        if (!unauthorizedTopics.isEmpty())
            return new TopicAuthorizationException(unauthorizedTopics);
        else if (!invalidTopics.isEmpty())
            return new InvalidTopicException(invalidTopics);
        else
            return null;
    }

    private KafkaException recoverableExceptionForTopic(String topic) {
        if (unauthorizedTopics.contains(topic))
            return new TopicAuthorizationException(Collections.singleton(topic));
        else if (invalidTopics.contains(topic))
            return new InvalidTopicException(Collections.singleton(topic));
        else
            return null;
    }

    private void clearRecoverableErrors() {
        invalidTopics = Collections.emptySet();
        unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
        this.attempts++;
        this.equivalentResponseCount = 0;
    }

    /**
     * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
     * Two examples are authentication and unsupported version exceptions.
     *
     * @param exception The fatal exception
     */
    public synchronized void fatalError(KafkaException exception) {
        this.fatalException = exception;
    }

    /**
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequest.Builder request = null;
        boolean isPartialUpdate = false;

        // Perform a partial update only if a full update hasn't been requested, and the last successful
        // hasn't exceeded the metadata refresh time.
        if (!this.needFullUpdate && this.lastSuccessfulRefreshMs + this.metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics();
            isPartialUpdate = true;
        }
        if (request == null) {
            request = newMetadataRequestBuilder();
            isPartialUpdate = false;
        }
        return new MetadataRequestAndVersion(request, requestVersion, isPartialUpdate);
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and all active topics.
     *
     * @return the constructed non-null metadata builder
     */
    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and any uncached topics,
     * otherwise null if the functionality is not supported.
     *
     * @return the constructed metadata builder, or null if not supported
     */
    protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return null;
    }

    /**
     * @return Mapping from topic IDs to topic names for all topics in the cache.
     */
    public Map<Uuid, String> topicNames() {
        return metadataSnapshot.topicNames();
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;
        public final boolean isPartialUpdate;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion,
                                          boolean isPartialUpdate) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

    /**
     * Represents current leader state known in metadata. It is possible that we know the leader, but not the
     * epoch if the metadata is received from a broker which does not support a sufficient Metadata API version.
     * It is also possible that we know of the leader epoch, but not the leader when it is derived
     * from an external source (e.g. a committed offset).
     */
    public static class LeaderAndEpoch {
        private static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Optional.empty(), Optional.empty());

        public final Optional<Node> leader;
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Optional<Node> leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) return false;
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    '}';
        }
    }

    public static class LeaderIdAndEpoch {
        public final Optional<Integer> leaderId;
        public final Optional<Integer> epoch;

        public LeaderIdAndEpoch(Optional<Integer> leaderId, Optional<Integer> epoch) {
            this.leaderId = Objects.requireNonNull(leaderId);
            this.epoch = Objects.requireNonNull(epoch);
        }

        @Override
        public String toString() {
            return "LeaderIdAndEpoch{" +
                "leaderId=" + leaderId.map(Number::toString).orElse("absent") +
                ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                '}';
        }
    }
}
