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
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.server.common.MetadataVersion.IBP_3_2_IV0
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, mutable}

abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange(): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state
    // except partitions that belong to topics to be deleted
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState(): Unit = {
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }

  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val isLeaderRecoverySupported = config.interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV0)

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Try to change the state of the given partitions to the given targetState, using the given
   * partitionLeaderElectionStrategyOpt if a leader election is required.
   * @param partitions The partitions
   * @param targetState The state
   * @param partitionLeaderElectionStrategyOpt The leader election strategy if a leader election is required.
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  override def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        result
      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * This API exercises(练习，使用，运用了) the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   *
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   *
   *         1️⃣ NewPartition：只是"登记户口"
   *         场景：Kafka 启动时，或者新创建了一个 Topic
   *         动作：
   *    1. 从ZooKeeper读取分区分配信息：/brokers/topics/{topic}/partitions/{partitionId}/replicas
   *    2. 将信息加载到 Controller 的内存缓存中
   *      controllerContext.putPartitionState(partition, NewPartition)
   *    3. 不做任何网络通信
   *      - 不选举 Leader（还不知道哪些副本在线）
   *      - 不发送请求（没有可通知的对象）
   *    // 初始状态：Controller 刚启动，内存是空的
   *    val partitions = Seq(new TopicPartition("my-topic", 0))
   *    // 转换为 NewPartition：
   *    //   - 从 ZK 读取：replicas = [0, 1, 2]
   *    //   - 存入缓存：controllerContext.partitionReplicaAssignment = [0, 1, 2]
   *    //   - 结束！不发送任何请求
   *    // 为什么？因为此时只是"知道了分区的存在"，还没有"激活"这个分区
   *
   * 2️⃣ OnlinePartition：真正"开业营业"
   *         情况 A：新分区首次上线 (NewPartition → OnlinePartition)
   *  1. 初始化 Leader 和 ISR
   *      - 选择第一个存活副本作为 Leader
   *      - 所有存活副本组成 ISR
   *  2. 写入 ZooKeeper：/controller/partitions/{topic}/{partition}/state
   *     { "leader": 0, "isr": [0,1,2], "epoch": 1 }
   *  3. ✅ 发送 LeaderAndIsrRequest
   *      - 给 ISR 中的所有副本：你们现在是一个团队了
   *      - isNew = true（告诉 follower 这是新建的分区）
   *  4. ✅ 发送 UpdateMetadataRequest
   *      - 给所有 Broker：这是新的分区元数据
   *  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
   *     leaderIsrAndControllerEpoch.leaderAndIsr.isr,  // 接收请求的副本列表
   *     partition,
   *     leaderIsrAndControllerEpoch,
   *     controllerContext.partitionFullReplicaAssignment(partition),
   *     isNew = true  // ← 标识这是新建分区
   *   )
   *  情况 B：已存在分区重新上线 (OfflinePartition → OnlinePartition)
   *   1. 选举新的 Leader（可能有多种策略）
   *   2. 更新 ZooKeeper 中的 LeaderAndIsr 信息
   *   3. ✅ 发送 LeaderAndIsrRequest
   *      - 通知相关副本新的 Leader 和 ISR 成员
   *      - isNew = false（这不是新建分区）
   *
   * 3️⃣ OfflinePartition：只是"暂停营业"
   *         场景：检测到某个副本宕机了
   *         动作：
   *   1. 在 Controller 内存中标记为 Offline
   *      controllerContext.putPartitionState(partition, OfflinePartition)
   *   2. ❌ 不发送 LeaderAndIsrRequest
   *      - 因为这个分区要下线了，没必要通知别人
   *      - 其他副本通过心跳检测自己会发现同伴消失了
   *   3. 如果需要 ISR 收缩，会在其他地方处理（不是在状态转换这里，而是在专门的 ISR 管理逻辑中）
   *   为什么 Offline 不发请求？
   *   想象一个场景：
   *    - 分区有副本 [0, 1, 2]
   *    - 副本 2 宕机了
   *    错误做法（如果 Offline 发请求）：
   *     Controller: "嘿，副本 2 下线了"
   *     副本 0、1: "？？？我们已经知道了啊"
   *     副本 2: "（已经宕机，收不到消息）"
   *     → 毫无意义的请求
   *    正确做法：
   *     - Controller 在内存中标记：副本 2 是 Offline
   *     - 后续选举 Leader 时会自动排除副本 2
   *     - 必要时发送 StopReplicaRequest 让其他副本停止向 2 同步
   *
   * 场景：Topic "orders" 有 3 个分区
   * ========== 阶段 1：Kafka 启动 ==========
   * Controller 启动，发现已有 Topic
   *  步骤 1：NewPartition
   *   partitions = ["orders-0", "orders-1", "orders-2"]
   *   doHandleStateChanges(partitions, NewPartition, None)
   *  执行结果：
   *   ✅ controllerContext 中有了分区信息
   *   ❌ 没有发送任何网络请求
   *   状态：NewPartition
   *
   * ========== 阶段 2：分区上线 ==========
   * 让分区开始服务
   * 步骤 2：OnlinePartition（首次）
   *    doHandleStateChanges(partitions, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
   * 执行结果：
   *  ✅ 选举 broker-0 为 orders-0 的 Leader
   *  ✅ 写入 ZK：/controller/partitions/orders/0/state
   *  ✅ 发送 LeaderAndIsrRequest 给 [broker-0, broker-1, broker-2]
   *  ✅ 发送 UpdateMetadataRequest 给所有 Broker
   *  状态：OnlinePartition
   *
   * ========== 阶段 3：副本故障 ==========
   * broker-2 宕机
   * 步骤 3：OfflinePartition
   *   doHandleStateChanges(Seq("orders-0"), OfflinePartition, None)
   * 执行结果：
   *   ✅ controllerContext 标记 orders-0 为 Offline
   *   ❌ 不发送任何请求
   *   状态：OfflinePartition
   *
   * ========== 阶段 4：重新上线 ==========
   * 修复了 orders-0 的问题
   * 步骤 4：OnlinePartition（重新上线）
   *   doHandleStateChanges(Seq("orders-0"), OnlinePartition, Some(PreferredReplicaPartitionLeaderElectionStrategy))
   * 执行结果：
   *   ✅ 重新选举 Leader（排除了 broker-2）
   *   ✅ 更新 ZK
   *   ✅ 发送 LeaderAndIsrRequest 给 [broker-0, broker-1]
   *   ✅ 发送 UpdateMetadataRequest
   *   状态：OnlinePartition
   *
   */
  private def doHandleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled

    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))

    /**
     * 核心设计原则：
     *  职责分离：每个状态只做自己该做的事
     *  最小化通信：只在必要时发送网络请求
     *  状态驱动：状态转换自然触发相应的动作
     * 这种设计避免了不必要的网络开销，同时保证了状态变更的一致性！
     */
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty
      case OnlinePartition =>
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)

        /**
         * 为什么只有 OnlinePartition 发请求？
         * | 状态 | 目的 | 是否发请求 | 原因 |
         * |------|------|-----------|------|
         * | **NewPartition** | 加载元数据到缓存 | ❌ | 只是"登记"，还没"激活" |
         * | **OnlinePartition** | 激活分区，提供服务 | ✅ | 需要通知副本团队组建完成 |
         * | **OfflinePartition** | 标记分区不可用 | ❌ | 都下线了，没必要通知 |
         * | **NonExistentPartition** | 删除分区状态 | ❌ | 分区都不存在了 |
         */
        if (uninitializedPartitions.nonEmpty) {
          // 初始化时候，默认ISR第一个副本为leader
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        //初始化的重新选举leader
        if (partitionsToElectLeader.nonEmpty) {
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )
          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
          }

          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition | NonExistentPartition =>
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          controllerContext.putPartitionState(partition, targetState)
        }
        Map.empty
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    //获取分区对应的副本
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    //分组：有存活分区的副本和没有存活分区的副本
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, _) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${controllerContext.partitionReplicaAssignment(partition).mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)//默认leader是第一个副本
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap

    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]

    while (remaining.nonEmpty) {
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      remaining = updatesToRetry

      finished.foreach {
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }

    finishedElections.toMap
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of two values:
   *         1. The partitions and the expected leader and isr that successfully had a leader elected. And exceptions
   *         corresponding to failed elections that should not be retried.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doElectLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    //已经初始化过的分区直接去Zookeeper读取数据
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }

          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            failedElections.put(partition, Left(exception))
        }

      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }

    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }

    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        //优先从 ISR 中选择：在副本分配列表 assignment 中，查找同时存在于存活副本 liveReplicas 和 ISR 列表 isr 中的第一个副本。
        //不洁选举：如果未找到符合条件的副本且启用了不洁选举（uncleanLeaderElectionEnabled），则从存活副本中选择第一个副本作为 Leader。
        leaderForOffline(
          controllerContext,
          isLeaderRecoverySupported,
          partitionsWithUncleanLeaderElectionState
        ).partition(_.leaderAndIsr.isEmpty)

      case ReassignPartitionLeaderElectionStrategy =>
        //在重分配列表 reassignment 中，查找同时存在于存活副本 liveReplicas 和 ISR 列表 isr 中的第一个副本。
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        //选择分区分配列表 assignment 中的第一个副本作为 Leader，前提是该副本存在于存活副本 liveReplicas 和 ISR 列表 isr 中。
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        //在副本分配列表 assignment 中，查找同时存在于存活副本 liveReplicas、ISR 列表 isr 中且不在关闭broker中的副本。
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    finishedUpdates.forKeyValue { (partition, result) =>
      result.foreach { leaderAndIsr =>
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicaAssignment, isNew = false)
      }
    }

    if (isDebugEnabled) {
      updatesToRetry.foreach { partition =>
        debug(s"Controller failed to elect leader for partition $partition. " +
          s"Attempted to write state ${adjustedLeaderAndIsrs(partition)}, but failed with bad ZK version. This will be retried.")
      }
    }

    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /**
   * For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   *
   * 1️⃣ 性能优化（allowUnclean 参数的作用）
   *         为什么需要 allowUnclean 参数？
   *         在某些场景下，系统已经确定要进行不洁选举（比如管理员手动触发），此时：
   *         ✅ 直接跳过 ZooKeeper 配置检查
   *         ✅ 避免网络 IO 开销
   *         ✅ 快速完成选举
   *         2️⃣ 细粒度控制（按主题配置）
   *         为什么要从 ZooKeeper 读取每个主题的配置？ 不同主题可能有不同的数据一致性要求
   *         主题 A：payment-transactions（支付交易）
   *    - 数据一致性至关重要
   *    - uncleanLeaderElectionEnable = false
   *
   *   主题 B：user-clicks（用户点击日志）
   *    - 可以容忍少量数据丢失
   *    - uncleanLeaderElectionEnable = true
   * 3️⃣ 容错处理（failed 映射）
   *  为什么要处理配置读取失败的情况？
   *     如果 ZooKeeper 临时不可用或配置节点不存在：采取保守策略：配置读取失败时，默认不允许不洁选举
   *  设计哲学：在不确定性面前，选择保护数据一致性（宁可不可用，也不丢失数据）
   *
   *  graph TD
   *         A[所有 ISR 副本下线] --> B{allowUnclean?}
   *         B -->|true| C[直接允许不洁选举]
   *         B -->|false| D[从 ZooKeeper 读取主题配置]
   *         D --> E{配置读取成功？}
   *         E -->|失败 | F[保守处理：禁止不洁选举]
   *         E -->|成功 | G{uncleanLeaderElectionEnable?}
   *         G -->|true| H[允许不洁选举]
   *         G -->|false| I[禁止不洁选举]
   *
   *  这个设计体现了 Kafka 的三个核心原则：
   *    灵活性：支持全局强制和按主题配置两种模式
   *    性能：通过 allowUnclean 参数避免不必要的 ZooKeeper 访问
   *    安全性：配置读取失败时采用保守策略，优先保护数据一致性
   *  这种设计让你可以根据业务需求，在可用性和数据一致性之间做出平衡。
   */
  private def collectUncleanLeaderElectionState(
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    allowUnclean: Boolean
  ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      // 情况 1：直接允许不洁选举（跳过配置检查
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true) // 第三个参数 true 表示允许不洁选举
      }
    } else { // 情况 2：需要检查每个主题的独立配置
      //获取主题对应的配置：/config/topics/$topic
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {// ZooKeeper 读取失败，保守处理：不允许不洁选举
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else { // 从 ZooKeeper 获取该主题的 uncleanLeaderElectionEnable 配置
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
    partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
      (partition, Option(leaderAndIsr), false)
    }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

object PartitionLeaderElectionAlgorithms {

  /**
   * 作用：为离线分区（所有副本都下线的分区）选举 Leader。
   * 逻辑：
   *  优先从 ISR 中选择：在副本分配列表 assignment 中，查找同时存在于存活副本 liveReplicas 和 ISR 列表 isr 中的第一个副本。
   *  不洁选举：如果未找到符合条件的副本且启用了不洁选举（uncleanLeaderElectionEnabled），则从存活副本中选择第一个副本作为 Leader。
   *  更新统计：如果进行了不洁选举，更新不洁选举的统计信息。
   */
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int],
                                     uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      if (uncleanLeaderElectionEnabled) {
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        None
      }
    }
  }

  /**
   * 作用：在分区重分配过程中选举 Leader。
   * 逻辑：在重分配列表 reassignment 中，查找同时存在于存活副本 liveReplicas 和 ISR 列表 isr 中的第一个副本。
   */
  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    //使用 find - 查找第一个符合条件的元素
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 作用：选举首选副本（通常是分区分配列表中的第一个副本）作为 Leader。
   * 逻辑：选择分区分配列表 assignment 中的第一个副本作为 Leader，前提是该副本存在于存活副本 liveReplicas 和 ISR 列表 isr 中。
   */
  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 使用 headOption.filter - 只检查第一个元素是否符合条件
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 作用：在受控关闭过程中选举 Leader。
   * 逻辑：在副本分配列表 assignment 中，查找同时存在于存活副本 liveReplicas、ISR 列表 isr 中且不在关闭broker中的副本。
   */
  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

sealed trait PartitionLeaderElectionStrategy
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy
case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy //Reassign:重分配
case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy //Preferred:更合意的
case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  def state: Byte
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
