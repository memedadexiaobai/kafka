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
package kafka.controller

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.TopicPartition

import scala.collection.Seq

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

/**
 * leader节点可能会发生的所有变动对应的方法
 */
object Election {

  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               isLeaderRecoverySupported: Boolean,
                               controllerContext: ControllerContext): ElectionResult = {
    //获取所有副本
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    //获取在线副本
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        val isr = leaderAndIsr.isr
        //选举leader
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(
          assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled, controllerContext)

        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)

          /**
           * 假设有一个分区有 3 个副本：[0, 1, 2]，ISR = [0, 1, 2]，Leader 是 0
           * 场景 A：正常故障转移（使用 newLeaderAndIsr）
           *  初始状态：
           *   Leader = 0, ISR = [0, 1, 2], RecoveryState = RECOVERED
           *  副本 0 宕机：
           *   选举副本 1 为新 Leader
           *   新 ISR = [1, 2] (过滤掉宕机的 0)
           *  判断：1 在旧 ISR [0,1,2] 中吗？✅ 是的
           *  结果：newLeaderAndIsr(1, [1, 2])
           *    → Leader = 1, ISR = [1, 2], RecoveryState = RECOVERED (保持不变)
           * 场景 B：不洁选举（使用 newRecoveringLeaderAndIsr）
           *  初始状态：
           *   Leader = 0, ISR = [0], 副本 1 和 2 都宕机了
           *  副本 0 也宕机了（所有 ISR 都下线）：
           *   启用不洁选举：允许从非 ISR 副本选举
           *   副本 1 上线并被选为 Leader
           *   新 ISR = [1] (只包含自己)
           *  判断：1 在旧 ISR [0] 中吗？❌ 不在
           *  结果：newRecoveringLeaderAndIsr(1, [1])
           *    → Leader = 1, ISR = [1], RecoveryState = RECOVERING ⚠️
           *
           * 为什么需要 RECOVERING 状态？
           *  数据一致性问题
           *  时间线：
           *   T1: Client 写入消息 M1 → 复制到 Leader(0), Follower(1)
           *   ISR = [0, 1], LEO(Leader End Offset) = 100
           *
           *   T2: Follower(1) 网络延迟，落后了
           *   Leader(0) LEO = 200, Follower(1) LEO = 150
           *   ISR 仍然是 [0, 1] (Kafka 会等待 lagging replica)
           *
           *   T3: 灾难发生 - 副本 0 立即宕机
           *   此时 ISR = [0] (副本 1 因为 lag 被移出 ISR)
           *
           *   T4: 不洁选举，副本 1 成为 Leader
           *   ❌ 问题：副本 1 只有 150 条消息，丢失了 150-200 的数据！
           *
           *   解决方案：
           *    标记为 RECOVERING 状态
           *    → 客户端知道这个分区数据可能不完整
           *    → 可能需要从其他备份恢复数据
           * isLeaderRecoverySupported：这个布尔值用于向后兼容：旧版本 Kafka 不支持 Leader 恢复状态检查
           *
           * | 维度 | `newRecoveringLeaderAndIsr` | `newLeaderAndIsr` |
           * |------|---------------------------|-------------------|
           * | **RecoveryState** | 设置为 `RECOVERING` | 保持原值 |
           * | **触发条件** | 新 Leader ∉ 旧 ISR | 新 Leader ∈ 旧 ISR |
           * | **数据安全性** | ⚠️ 可能丢失数据 | ✅ 数据安全 |
           * | **选举类型** | 不洁选举 | 正常选举 |
           * | **后续操作** | 需要数据恢复 | 直接提供服务 |
           *
           * 这个设计体现了 Kafka 在 **可用性** 和 **数据一致性** 之间的精细权衡：
           * 即使允许不洁选举提高可用性，也要通过 `RECOVERING` 状态标记潜在的数据风险。
           */
          if (!isr.contains(leader) && isLeaderRecoverySupported) {
            /**
             * The new leader is not in the old ISR so mark the partition a RECOVERING
             * 情况 1：新 Leader 不在旧 ISR 中 → 标记为 RECOVERING（恢复中）
             * 不洁选举后的恢复:
             *  特点：
             *   🚨 设置 leaderRecoveryState = RECOVERING
             *   📍 触发场景：新 Leader 不在旧的 ISR 列表中
             *   ⚠️ 这意味着发生了 不洁选举（unclean leader election）
             *   🔄 新 Leader 的数据可能比其他副本旧，需要数据恢复
             */
            leaderAndIsr.newRecoveringLeaderAndIsr(leader, newIsr)
          } else {
            /**
             * Elect a new leader but keep the previous leader recovery state
             * 情况 2：新 Leader 在旧 ISR 中 → 保持之前的恢复状态
             * 正常选举
             *  特点：
             *   ✅ 保持原有的 leaderRecoveryState（可能是 RECOVERED 或 RECOVERING）
             *   📍 触发场景：新 Leader 在旧的 ISR 列表中
             *   ✔️ 这是安全的选举，新 Leader 的数据是最新的
             */
            leaderAndIsr.newLeaderAndIsr(leader, newIsr)
          }
        }
        ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)

      case None =>
        ElectionResult(partition, None, liveReplicas)
    }
  }

  /**
   * Elect leaders for new or offline partitions.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param isLeaderRecoverySupported true leader recovery is support and should be set if election is unclean
   * @param partitionsWithUncleanLeaderRecoveryState A sequence of tuples representing the partitions
   *                                                 that need election, their leader/ISR state, and whether
   *                                                 or not unclean leader election is enabled
   *
   * @return The election results
   */
  def leaderForOffline(
    controllerContext: ControllerContext,
    isLeaderRecoverySupported: Boolean,
    partitionsWithUncleanLeaderRecoveryState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)]
  ): Seq[ElectionResult] = {
    partitionsWithUncleanLeaderRecoveryState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled, isLeaderRecoverySupported, controllerContext)
    }
  }

  private def leaderForReassign(partition: TopicPartition,
                                leaderAndIsr: LeaderAndIsr,
                                controllerContext: ControllerContext): ElectionResult = {
    //获取最终的目标状态
    val targetReplicas = controllerContext.partitionFullReplicaAssignment(partition).targetReplicas
    val liveReplicas = targetReplicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(targetReplicas, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, targetReplicas)
  }

  /**
   * Elect leaders for partitions that are undergoing reassignment.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForReassign(controllerContext: ControllerContext,
                        leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForReassign(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
  }

  /**
   * Elect preferred leaders.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForControlledShutdown(partition: TopicPartition,
                                          leaderAndIsr: LeaderAndIsr,
                                          shuttingDownBrokerIds: Set[Int],
                                          controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveOrShuttingDownReplicas = assignment.filter(replica =>
      controllerContext.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
  }

  /**
   * Elect leaders for partitions whose current leaders are shutting down.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIds, controllerContext)
    }
  }
}
