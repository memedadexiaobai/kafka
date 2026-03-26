/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing replica state")
    initializeReplicaState()
    info("Triggering online replica state changes")
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica)
    info("Triggering offline replica state changes")
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica)
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState(): Unit = {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
}

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 *                        ReplicaDeletionStarted and OfflineReplica
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ZkReplicaStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            zkClient: KafkaZkClient,
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends ReplicaStateMachine(controllerContext) with Logging {

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          doHandleStateChanges(replicaId, replicas, targetState)
        }
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked
   * @param replicas The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   *
   * `isNew = false` vs `isNew = true` 的区别
   *
   *  | 参数 | 含义 | 使用场景 |
   *  |------|------|----------|
   *  | `isNew = true` | **新副本**，需要从 Leader 开始同步数据 | OfflineReplica → NewReplica |
   *  | `isNew = false` | **现有副本**，只是状态变更，数据已完整 | 各种状态 → OnlineReplica |
   */
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))

    targetState match {
      case NewReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          val currentState = controllerContext.replicaState(replica)

          /**
           * 假设有 3 个 Broker 的分区:
           *  初始状态:
           *   - Leader: broker-1
           *   - ISR: [broker-1, broker-2, broker-3]
           *   - broker-3 突然离线
           *
           *   broker-3 离线期间:
           *    - ISR: [broker-1, broker-2]  (broker-3 被移除)
           *
           *   broker-3 恢复时:
           *    1. 控制器检测到 broker-3 上线
           *    2. 尝试将 broker-3 从 OfflineReplica → NewReplica
           *    3. 检查发现 broker-3 不是 Leader ✓
           *    4. 发送 LeaderAndIsrRequest(broker-3, isNew=true)
           *    5. broker-3 开始从 broker-1 同步数据
           *    6. 同步完成后进入 OnlineReplica 状态
           * 这段代码的核心设计原则是：
           *   防止错误的状态转换（Leader 不能变成 NewReplica）
           *   确保数据一致性（恢复的副本必须同步最新数据）
           *   处理边界情况（没有 Leader 时的降级处理）
           *  这样设计保证了 Kafka 在副本故障恢复时的可靠性和数据一致性。
           */
          controllerContext.partitionLeadershipInfo(partition) match {
            case Some(leaderIsrAndControllerEpoch) =>
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                /**
                 * 如果一个副本当前是 Partition 的 Leader，它不应该进入 NewReplica 状态
                 * NewReplica 状态的含义是：这个副本需要从 Leader 同步数据的新加入副本
                 * 但 Leader 本身已经有完整数据，不需要同步
                 * 这种情况通常意味着元数据不一致，所以记录失败日志到 OfflineReplica 状态
                 * 示例场景：
                 *  假设分区 partition-0 的 Leader 是 broker-1
                 *  如果 broker-1 因为网络问题短暂离线，然后快速恢复
                 *  控制器可能尝试将 broker-1 的副本移到 NewReplica 状态
                 *  但 broker-1 本身就是 Leader，这显然是错误的状态转换
                 */
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else {
                /**
                 * 为什么发送这个请求？
                 *  告诉恢复的副本："你现在是 ISR 的一员了，这是当前的 Leader 和 ISR 信息"
                 *  isNew = true 表示这是一个新加入的副本，需要从 Leader 开始同步数据
                 *  只有非 Leader 副本才需要这个请求
                 */
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled)
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
                controllerContext.putReplicaState(replica, NewReplica)
              }
            case None =>
              if (traceEnabled) {
                /**
                 * 为什么可以直接成功？
                 *  partitionLeadershipInfo(partition) 返回 None 表示：这个分区还没有选举出 Leader
                 *  可能发生在：分区刚创建、所有副本都刚启动等场景
                 *  此时副本可以先标记为 NewReplica 状态，等待 Leader 选举完成后再处理
                 */
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              }
              controllerContext.putReplicaState(replica, NewReplica)
          }
        }
      case OnlineReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          val currentState = controllerContext.replicaState(replica)

          /**
           * 初始状态:
           * - broker-1: Leader
           * - broker-2: OnlineReplica (ISR: [1, 2])
           * - broker-3: OfflineReplica (因故障离线)
           *
           * === 场景 A: 正常流程 ===
           * broker-3 恢复:
           * 1. OfflineReplica → NewReplica (发请求，isNew=true)
           * "你是新副本，需要同步数据"
           * 2. broker-3 从 broker-1 同步数据完成
           * 3. NewReplica → OnlineReplica (不发请求)
           * "你已经同步完了，可以正式提供服务了"
           * 因为 broker-3 早就知道 ISR 信息了，不需要重复通知
           *
           * === 场景 B: 跳过 NewReplica 的情况 ===
           * 某些特殊情况下（比如控制器重启、元数据修复）:
           * 1. broker-3 直接从 OfflineReplica → OnlineReplica
           * 2. 这时必须发 LeaderAndIsrRequest(broker-3, isNew=false)
           * 告诉 broker-3:
           * - "你现在是 ISR 的正式成员了"
           * - "这是当前的 Leader 和 ISR 列表"
           * - "你已经有最新数据了，可以直接服务请求"
           */
          currentState match {
            case NewReplica =>
              // 场景 1: 从 NewReplica → OnlineReplica
              // 说明副本已经完成数据同步，可以正常提供服务了
              // 不需要发请求，因为之前在 NewReplica 阶段已经发过了
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              if (!assignment.replicas.contains(replicaId)) {
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              }
            case _ => // 其他所有状态（OfflineReplica、NonExistentReplica 等）
              // 场景 2: 从其他状态直接跳到 OnlineReplica
              controllerContext.partitionLeadershipInfo(partition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  //核心原因：通知副本"你已经是正式成员了"
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)
          controllerContext.putReplicaState(replica, OnlineReplica)
        }
      case OfflineReplica =>
        //暂停副本同步
        validReplicas.foreach { replica =>
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        }
        //分组：1. 有 Leader 信息的副本 2. 无 Leader 信息的副本
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        }
        // 移除 ISR 中对应的副本并且返回最新的 leaderIsrAndEpoch
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
            // 给其他副本发送 LeaderAndIsrRequest 告知副本
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }
          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)

          /**
           * // 1. 从 ISR 中移除故障副本（需要更新 ZooKeeper）
           * // 2. 给其他副本发送 LeaderAndIsrRequest（告知新的 ISR）
           * // 3. 更新本地状态
           */
          controllerContext.putReplicaState(replica, OfflineReplica)
        }

        /**
         * 核心原因：这些分区处于特殊状态，无法执行标准的 ISR 移除流程
         * 1️⃣ 什么是 replicasWithoutLeadershipInfo？
         *    replicasWithoutLeadershipInfo = 没有 Leader 信息的分区 即 partitionLeadershipInfo(partition) 返回 None
         * 2️⃣ 什么情况下会没有 Leader 信息？
         *  场景 A：ZooKeeper 中没有该分区的状态记录
         *   case Code.NONODE =>
         *    partitionsWithNoLeaderAndIsrInZk += partition
         *  原因：
         *   分区刚创建，还没来得及写入 ZooKeeper
         *   Controller 重启后，元数据还没完全恢复
         *   ZooKeeper节点被意外删除
         *  场景 B：分区正在被删除
         *   if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
         *    val exception = new StateChangeFailedException(...)
         *    // 如果 Topic 正在被删除，就不报错了
         *   }
         *  3️⃣ 为什么直接更新状态？对比两个分支
         *    updatedLeaderIsrAndControllerEpochs:
         *      // 1. 从 ISR 中移除故障副本（需要更新 ZooKeeper）
         *      // 2. 给其他副本发送 LeaderAndIsrRequest（告知新的 ISR）
         *     // 3. 更新本地状态
         *   流程复杂：
         *    ✅ 可以更新 ISR（因为 ZooKeeper 中有记录）
         *    ✅ 可以通知其他副本（因为有 Leader 信息）
         *  没有 Leader 信息的分支:
         *    1. 只发送 UpdateMetadata 请求（让其他 Broker 知道这个分区）
         *    2. 直接更新本地状态
         *  为什么简化处理？
         *   | 原因 | 说明 |
         *   |------|------|
         *   | ❌ **无法更新 ISR** | ZooKeeper 中没有 LeaderAndIsr 记录，无法执行 `removeReplicasFromIsr` |
         *   | ❌ **无法通知特定副本** | 不知道谁是 Leader，无法发送 `LeaderAndIsrRequest` |
         *   | ✅ **但可以广播元数据** | 发送 `UpdateMetadataRequest` 给所有 Broker，告诉大家"这个分区存在" |
         *   | ✅ **必须更新本地状态** | 控制器需要在内存中标记这个副本为 Offline |
         * 4️⃣ 具体示例场景
         *   场景 1：分区刚创建，Broker 就挂了
         *   时间线:
         *   T1: 管理员创建 topic-0, partition-0
         *     - 分配副本：[broker-1, broker-2, broker-3]
         *     - 计划 Leader: broker-1
         *
         *   T2: Controller 开始初始化分区状态
         *     - 还没来得及写 ZooKeeper
         *
         *   T3: broker-3 突然宕机
         *
         *   T4: Controller 检测到 broker-3 离线
         *    - 尝试将 broker-3 的副本移到 OfflineReplica
         *    - 查询 partitionLeadershipInfo(partition-0) = None ❌
         *    (因为 ZooKeeper 中还没有记录)
         *
         *   T5: 进入 replicasWithoutLeadershipInfo 分支
         *     - 发送 UpdateMetadataRequest 给所有 Broker
         *       "嘿，大家注意一下，有个分区 topic-0/partition-0"
         *     - 直接标记 broker-3 的副本为 OfflineReplica
         *
         * 场景 2：Controller 故障转移期间
         *   初始 Controller (broker-0) 宕机
         *   新 Controller (broker-1) 选举成功
         *
         *   新 Controller 加载元数据:
         *    - 从 ZooKeeper 读取分区信息
         *    - 但某些分区的 LeaderAndIsr 路径可能还在恢复中
         *
         *   此时如果有副本故障:
         *      - 查询不到 Leader 信息
         *      - 只能走简化流程
         *
         * 5️⃣ 为什么发送 UpdateMetadataRequest？
         *   同步分区元数据：让所有 Broker 知道这个分区的存在和配置
         *   触发重新发现：其他 Broker 收到后会检查自己的状态
         *   等待后续修复：等 Controller 完成分区初始化后，会再次触发正常的 ISR 更新流程
         *  为什么不发 LeaderAndIsrRequest？
         *    因为没有 Leader 信息可传递
         *    发了也是空的或者错误的
         *
         * 6️⃣ 这样做的安全性保证
         *   直接更新状态会不会丢失数据？
         *    不会！ 因为：StopReplicaRequest 已经发送，故障副本已经停止工作
         *    只是本地状态标记
         *       Controller 在内存中标记为 Offline
         *       不影响其他正常副本
         *    后续会修复
         *     一旦分区有了 Leader 信息，会触发正常的 ISR 收缩流程
         *     ZooKeeper 中的状态最终会一致
         *
         * | 分支 | 处理方式 | 原因 |
         * |------|---------|------|
         * | **有 Leader 信息** | 更新 ISR → 通知其他副本 → 更新状态 | 完整流程，确保一致性 |
         * | **无 Leader 信息** | 广播元数据 → 直接更新状态 | **降级处理**，因为无法执行标准流程 |
         *
         * 设计哲学：
         *  "能做多少做多少，不要因为部分失败而阻塞整个流程"
         * 这是一种务实的容错设计，保证了即使在元数据不完整的情况下，系统仍能继续运行。
         */
        replicasWithoutLeadershipInfo.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)
          // 1. 只发送 UpdateMetadata 请求（让其他 Broker 知道这个分区）
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition))
          // 2. 直接更新本地状态
          controllerContext.putReplicaState(replica, OfflineReplica)
        }
        // 有leader的ISR收缩 没有leader的发个数据更新请求 重新拉取下数据
      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
        /**
         * 这2个不发请求:核心原因：这两个状态是中间结果状态，不需要额外操作
         * // 完整的删除流程
         * OnlineReplica
         *   ↓ (故障/下线)
         * OfflineReplica              ← 发送 StopReplicaRequest (delete=false)
         *   ↓ (开始删除)               ← 从 ISR 移除并通知其他副本
         * ReplicaDeletionStarted      ← 发送 StopReplicaRequest (delete=true) ⭐
         * ├─→ ReplicaDeletionSuccessful  ← 只更新本地状态 ✅
         * │       ↓
         * │   NonExistentReplica    ← 清理元数据
         * │
         * └─→ ReplicaDeletionIneligible ← 只更新本地状态 ✅
         * 2️⃣ 为什么这两个状态不发请求？
         *   ReplicaDeletionIneligible（删除不合格）：删除失败了，需要标记为"不合格"
         *    | 原因 | 说明 |
         *    |------|------|
         *    | ✅ **已经发过 StopReplicaRequest** | 在 `ReplicaDeletionStarted` 阶段 已经发送了带 `delete=true` 的请求 |
         *    | ❌ **Broker 无响应或失败** | 副本没有响应、网络超时、或其他错误导致删除失败 |
         *    | 📝 **只是标记状态** | Controller 需要在内存中标记这个副本"删除失败，稍后重试" |
         *    | 🔄 **等待下次重试** | 后续会有其他机制重新尝试删除 |
         *    T1: Controller → broker-3: StopReplicaRequest(delete=true)
         *    T2: broker-3 网络超时，没有响应
         *    T3: Controller 标记 broker-3 为 ReplicaDeletionIneligible
         *    T4: Controller 稍后会重试删除流程
         *  ReplicaDeletionSuccessful（删除成功）
         *   | 原因 | 说明 |
         *   |------|------|
         *   | ✅ **StopReplicaRequest 已成功** | Broker 已经成功删除了副本数据 |
         *   | ✅ **Broker 已确认** | 收到了 Broker 的成功响应 |
         *   | 📝 **只需记录成功状态** | Controller 在内存中标记"这个副本已删除" |
         *   | ➡️ **下一步是清理元数据** | 等待转换到 `NonExistentReplica` 状态 |
         *   T1: Controller → broker-3: StopReplicaRequest(delete=true)
         *   T2: broker-3 → Controller: 删除成功 ✓
         *   T3: Controller 标记 broker-3 为 ReplicaDeletionSuccessful
         *   T4: Controller 将 broker-3 从分区分配列表中移除
         * 3️⃣ 关键对比：哪些状态发了请求？
         *  | 状态 | 是否发请求 | 发的什么请求 | 为什么 |
         *  |------|-----------|-------------|--------|
         *  | **OfflineReplica** | ✅ 发 | StopReplicaRequest + LeaderAndIsrRequest | 停止副本同步 + 从 ISR 移除 |
         *  | **ReplicaDeletionStarted** | ✅ 发 | StopReplicaRequest(delete=true) | 真正执行删除操作 |
         *  | **ReplicaDeletionIneligible** | ❌ 不发 | - | 已经发过了，只是标记失败 |
         *  | **ReplicaDeletionSuccessful** | ❌ 不发 | - | 已经发过了，只是标记成功 |
         *  | **NonExistentReplica** | ❌ 不发 | - | 只清理本地元数据 |
         * 4️⃣ 完整时序图示例
         * 假设要删除 broker-3 上的副本：
         * 时间线                    Controller                Broker-3
         * --------------------------------------------------------------
         * T1: 检测到需要删除         OfflineReplica
         * ↓
         * [发送 StopReplicaRequest(delete=false)]
         * ↓                        停止作为 Follower
         *
         * T2: 开始删除              ReplicaDeletionStarted
         * ↓
         * [发送 StopReplicaRequest(delete=true)] ⭐
         * ↓                        删除本地数据
         * ↓                       ↓
         * ↓                       返回成功 ✓
         *
         * T3: 处理结果              ReplicaDeletionSuccessful
         * ↓                        (已收到响应)
         * [只更新本地状态]
         * ↓
         *
         * T4: 清理元数据            NonExistentReplica
         * ↓
         * [从分区分配列表移除]
         *如果删除失败：
         * 时间线                    Controller                Broker-3
         * --------------------------------------------------------------
         * T1-T2: 同上              ReplicaDeletionStarted
         * ↓
         * [发送 StopReplicaRequest(delete=true)]
         * ↓                        (网络超时/失败)
         * ↓                       ❌ 无响应
         *
         * T3: 处理失败              ReplicaDeletionIneligible
         * ↓
         * [只更新本地状态]
         * ↓
         * [等待稍后重试]
         *
         * ReplicaDeletionIneligible：
         *  ✅ 在 ReplicaDeletionStarted 阶段已经发送了删除请求
         *  📝 现在是标记"删除失败，需要重试"的状态
         *  🔄 不需要额外操作，等待重试机制
         * ReplicaDeletionSuccessful：
         *  ✅ 在 ReplicaDeletionStarted 阶段已经发送了删除请求
         *  ✅ 已经收到了 Broker 的成功响应
         *  📝 现在只需要记录"删除成功"
         *  ➡️ 下一步是清理元数据（转到 NonExistentReplica）
         *
         * 设计哲学：
         *  "每个状态只做自己该做的事，不要重复操作"
         *  ReplicaDeletionStarted：执行删除（发请求）
         *  ReplicaDeletionIneligible/Successful：记录结果（更新状态）
         */
      case NonExistentReplica =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          val newAssignedReplicas = controllerContext
            .partitionFullReplicaAssignment(replica.topicPartition)
            .removeReplica(replica.replica)

          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)
          controllerContext.removeReplicaState(replica)
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   */
  private def removeReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      // finishedRemoval:已经处理完成的 removalsToRetry：需要重试的
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      remaining = removalsToRetry

      finishedRemoval.foreach {
        case (partition, Left(e)) =>
            val replica = PartitionAndReplica(partition, replicaId) //主题和分区
            val currentState = controllerContext.replicaState(replica)//获取副本当前状态
            logFailedStateChange(replica, currentState, OfflineReplica, e)
        case (partition, Right(leaderIsrAndEpoch)) =>
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *         removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *         not be retried
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doRemoveReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {
    // 一个是leaderAndIsrs 另一个是没有leaderAndIsrs和分区
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)
    // 根据 replicaId 是否在ISR中再分组
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          /**
           * 假设分区 partition-0 的 ISR = [broker-3]
           * 现在要将 broker-3 从 ISR 中移除
           *
           * 如果直接 filter: ISR.filter(_ != 3) = []  // 空列表！
           *
           * 问题来了：
           * - ISR 为空意味着这个分区没有可用的副本
           * - Kafka 不允许 ISR 为空（否则分区不可用）
           * - 所以保持原样 ISR = [broker-3]
           * - 但会将 Leader 设置为 NoLeader（由上一行代码处理）
           *
           * 设计意图：
           *  当 ISR 只剩一个副本且这个副本要下线时
           *  保留它在 ISR 中，但标记为"没有 Leader"
           *  这样可以触发新的 Leader 选举流程，而不是让分区直接不可用
           * 这样设计保证了：
           *  1. **多副本场景**：正常移除故障副本
           *  2. **单副本场景**：保留 ISR 但标记无 Leader，触发恢复机制
           *  3. **数据一致性**：ZooKeeper 中的 ISR 永远不为空
           */
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }

    // finishedPartitions：已经处理完的分区 updatesToRetry：需要重试的分区
    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
            "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap

    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }

    if (isDebugEnabled) {
      updatesToRetry.foreach { partition =>
        debug(s"Controller failed to remove replica $replicaId from ISR of partition $partition. " +
          s"Attempted to write state ${adjustedLeaderAndIsrs(partition)}, but failed with bad ZK version. This will be retried.")
      }
    }

    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *         The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   */
  private def getTopicPartitionStatesFromZk(
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }

  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

sealed trait ReplicaState {
  def state: Byte
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
