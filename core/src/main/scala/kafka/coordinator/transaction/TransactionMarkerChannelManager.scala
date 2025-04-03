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
package kafka.coordinator.transaction


import kafka.coordinator.transaction.TransactionMarkerChannelManager.{LogAppendRetryQueueSizeMetricName, MetricNames, UnknownDestinationQueueSizeMetricName}

import java.util
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}
import kafka.server.{KafkaConfig, MetadataCache, RequestLocal}
import kafka.utils.Implicits._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Node, Reconfigurable, TopicPartition}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import scala.collection.{concurrent, immutable}
import scala.jdk.CollectionConverters._

/**
 * TransactionMarkerChannelManager 是一个与消息队列和事务消息处理相关的组件。虽然参考资料中没有直接提到 TransactionMarkerChannelManager，但根据其名称和通常的命名惯例，我们可以推测它的主要职责如下：
 * 管理事务标记通道：TransactionMarkerChannelManager 可能负责管理和维护事务消息的标记通道。事务消息是指那些需要在发送和提交之间保持一致性的消息，通常用于分布式事务场景。
 * 事务消息的生命周期管理：它可能负责处理事务消息的生命周期，包括事务的开始、提交和回滚。这涉及到在消息队列中设置和检查事务标记，以确保消息的可靠性和一致性。
 * 协调生产者和消费者：在事务消息的发送过程中，TransactionMarkerChannelManager 可能协调生产者和消费者之间的通信，确保事务消息在生产者提交事务之前不会被消费者消费。
 * 错误处理和重试机制：它可能还负责处理事务消息发送过程中的错误，并提供重试机制，以确保事务的最终一致性。
 */
object TransactionMarkerChannelManager {
  private val UnknownDestinationQueueSizeMetricName = "UnknownDestinationQueueSize"
  private val LogAppendRetryQueueSizeMetricName = "LogAppendRetryQueueSize"

  // Visible for testing
  private[transaction] val MetricNames = Set(
    UnknownDestinationQueueSizeMetricName,
    LogAppendRetryQueueSizeMetricName
  )

  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            txnStateManager: TransactionStateManager,
            time: Time,
            logContext: LogContext): TransactionMarkerChannelManager = {

    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )

    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }

    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "txn-marker-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder,
      logContext
    )
    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      s"broker-${config.brokerId}-txn-marker-sender",
      1,
      50,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      false,
      new ApiVersions,
      logContext,
      MetadataRecoveryStrategy.NONE
    )

    new TransactionMarkerChannelManager(config,
      metadataCache,
      networkClient,
      txnStateManager,
      time
    )
  }

}

class TxnMarkerQueue(@volatile var destination: Node) extends Logging {

  // keep track of the requests per txn topic partition so we can easily clear the queue during partition emigration
  private val markersPerTxnTopicPartition = new ConcurrentHashMap[Int, BlockingQueue[PendingCompleteTxnAndMarkerEntry]]().asScala

  def removeMarkersForTxnTopicPartition(partition: Int): Option[BlockingQueue[PendingCompleteTxnAndMarkerEntry]] = {
    markersPerTxnTopicPartition.remove(partition)
  }

  def addMarkers(txnTopicPartition: Int, pendingCompleteTxnAndMarker: PendingCompleteTxnAndMarkerEntry): Unit = {
    val queue = CoreUtils.atomicGetOrUpdate(markersPerTxnTopicPartition, txnTopicPartition, {
      // Note that this may get called more than once if threads have a close race while adding new queue.
      info(s"Creating new marker queue for txn partition $txnTopicPartition to destination broker ${destination.id}")
      new LinkedBlockingQueue[PendingCompleteTxnAndMarkerEntry]()
    })
    queue.add(pendingCompleteTxnAndMarker)

    if (markersPerTxnTopicPartition.get(txnTopicPartition).orNull != queue) {
      // This could happen if the queue got removed concurrently.
      // Note that it could create an unexpected state when the queue is removed from
      // removeMarkersForTxnTopicPartition, we could have:
      //
      // 1. [addMarkers] Retrieve queue.
      // 2. [removeMarkersForTxnTopicPartition] Remove queue.
      // 3. [removeMarkersForTxnTopicPartition] Iterate over queue, but not removeMarkersForTxn because queue is empty.
      // 4. [addMarkers] Add markers to the queue.
      //
      // Now we've effectively removed the markers while transactionsWithPendingMarkers has an entry.
      //
      // While this could lead to an orphan entry in transactionsWithPendingMarkers, sending new markers
      // will fix the state, so it shouldn't impact the state machine operation.
      warn(s"Added $pendingCompleteTxnAndMarker to dead queue for txn partition $txnTopicPartition to destination broker ${destination.id}")
    }
  }

  def forEachTxnTopicPartition[B](f:(Int, BlockingQueue[PendingCompleteTxnAndMarkerEntry]) => B): Unit =
    markersPerTxnTopicPartition.forKeyValue { (partition, queue) =>
      if (!queue.isEmpty) f(partition, queue)
    }

  def totalNumMarkers: Int = markersPerTxnTopicPartition.values.foldLeft(0) { _ + _.size }

  // visible for testing
  def totalNumMarkers(txnTopicPartition: Int): Int = markersPerTxnTopicPartition.get(txnTopicPartition).fold(0)(_.size)
}

class TransactionMarkerChannelManager(
  config: KafkaConfig,
  metadataCache: MetadataCache,
  networkClient: NetworkClient,
  txnStateManager: TransactionStateManager,
  time: Time
) extends InterBrokerSendThread("TxnMarkerSenderThread-" + config.brokerId, networkClient, config.requestTimeoutMs, time)
  with Logging {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  this.logIdent = "[Transaction Marker Channel Manager " + config.brokerId + "]: "

  private val interBrokerListenerName: ListenerName = config.interBrokerListenerName

  private val markersQueuePerBroker: concurrent.Map[Int, TxnMarkerQueue] = new ConcurrentHashMap[Int, TxnMarkerQueue]().asScala

  private val markersQueueForUnknownBroker = new TxnMarkerQueue(Node.noNode)

  private val txnLogAppendRetryQueue = new LinkedBlockingQueue[PendingCompleteTxn]()

  private val transactionsWithPendingMarkers = new ConcurrentHashMap[String, PendingCompleteTxn]

  metricsGroup.newGauge(UnknownDestinationQueueSizeMetricName, () => markersQueueForUnknownBroker.totalNumMarkers)
  metricsGroup.newGauge(LogAppendRetryQueueSizeMetricName, () => txnLogAppendRetryQueue.size)

  override def shutdown(): Unit = {
    try {
      super.shutdown()
      markersQueuePerBroker.clear()
    } finally {
      removeMetrics()
    }
  }

  private def removeMetrics(): Unit = {
    MetricNames.foreach(metricsGroup.removeMetric)
  }

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    markersQueuePerBroker.get(brokerId)
  }

  // visible for testing
  private[transaction] def queueForUnknownBroker = markersQueueForUnknownBroker

  private[transaction] def addMarkersForBroker(broker: Node, txnTopicPartition: Int, pendingCompleteTxnAndMarker: PendingCompleteTxnAndMarkerEntry): Unit = {
    val brokerId = broker.id

    // we do not synchronize on the update of the broker node with the enqueuing,
    // since even if there is a race condition we will just retry
    val brokerRequestQueue = CoreUtils.atomicGetOrUpdate(markersQueuePerBroker, brokerId, {
      // Note that this may get called more than once if threads have a close race while adding new queue.
      info(s"Creating new marker queue map to destination broker $brokerId")
      new TxnMarkerQueue(broker)
    })
    brokerRequestQueue.destination = broker
    brokerRequestQueue.addMarkers(txnTopicPartition, pendingCompleteTxnAndMarker)

    trace(s"Added marker ${pendingCompleteTxnAndMarker.txnMarkerEntry} for transactional id" +
      s" ${pendingCompleteTxnAndMarker.pendingCompleteTxn.transactionalId} to destination broker $brokerId")
  }

  private def retryLogAppends(): Unit = {
    val txnLogAppendRetries: util.List[PendingCompleteTxn] = new util.ArrayList[PendingCompleteTxn]()
    //LinkedBlockingQueue 中的 drainTo 方法用于将队列中的所有可用元素转移到另一个集合中。
    // 这个方法在处理大量消息或任务时非常有用，可以一次性批量处理队列中的元素，而不是逐个处理。
    // public int drainTo(Collection<? super E> c)
    // c: 目标集合，接收从队列中移除的元素。该集合的元素类型必须能够容纳队列中的元素类型
    // 返回值：返回从队列中移除并转移到目标集合的元素数量
    //特点
    // 1.非阻塞：drainTo 方法是非阻塞的，它会立即返回，不会等待队列中有更多元素可用。如果队列为空，它会立即返回 0。
    // 2.批量处理：可以一次性处理多个元素，提高处理效率。
    // 3.线程安全：drainTo 方法是线程安全的，可以在多线程环境中使用。
    txnLogAppendRetryQueue.drainTo(txnLogAppendRetries)
    txnLogAppendRetries.forEach { txnLogAppend =>
      debug(s"Retry appending $txnLogAppend transaction log")
      tryAppendToLog(txnLogAppend)
    }
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    //把txnLogAppendRetryQueue中的日志写试下追加落库
    retryLogAppends()

    val pendingCompleteTxnAndMarkerEntries = new util.ArrayList[PendingCompleteTxnAndMarkerEntry]()
    markersQueueForUnknownBroker.forEachTxnTopicPartition { case (_, queue) =>
      queue.drainTo(pendingCompleteTxnAndMarkerEntries)
    }

    for (pendingCompleteTxnAndMarker: PendingCompleteTxnAndMarkerEntry <- pendingCompleteTxnAndMarkerEntries.asScala) {
      val producerId = pendingCompleteTxnAndMarker.txnMarkerEntry.producerId
      val producerEpoch = pendingCompleteTxnAndMarker.txnMarkerEntry.producerEpoch
      val txnResult = pendingCompleteTxnAndMarker.txnMarkerEntry.transactionResult
      val pendingCompleteTxn = pendingCompleteTxnAndMarker.pendingCompleteTxn
      val topicPartitions = pendingCompleteTxnAndMarker.txnMarkerEntry.partitions.asScala.toSet

      addTxnMarkersToBrokerQueue(producerId, producerEpoch, txnResult, pendingCompleteTxn, topicPartitions)
    }

    val currentTimeMs = time.milliseconds()
    markersQueuePerBroker.values.map { brokerRequestQueue =>
      val pendingCompleteTxnAndMarkerEntries = new util.ArrayList[PendingCompleteTxnAndMarkerEntry]()
      brokerRequestQueue.forEachTxnTopicPartition { case (_, queue) =>
        queue.drainTo(pendingCompleteTxnAndMarkerEntries)
      }
      (brokerRequestQueue.destination, pendingCompleteTxnAndMarkerEntries)
    }.filter { case (_, entries) => !entries.isEmpty }.map { case (node, entries) =>
      val markersToSend = entries.asScala.map(_.txnMarkerEntry).asJava
      val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(node.id, txnStateManager, this, entries)
      val request = new WriteTxnMarkersRequest.Builder(
        metadataCache.metadataVersion().writeTxnMarkersRequestVersion(), markersToSend
      )

      new RequestAndCompletionHandler(
        currentTimeMs,
        node,
        request,
        requestCompletionHandler
      )
    }.asJavaCollection
  }

  private def writeTxnCompletion(pendingCompleteTxn: PendingCompleteTxn): Unit = {
    val transactionalId = pendingCompleteTxn.transactionalId
    val txnMetadata = pendingCompleteTxn.txnMetadata
    val newMetadata = pendingCompleteTxn.newMetadata
    val coordinatorEpoch = pendingCompleteTxn.coordinatorEpoch

    trace(s"Completed sending transaction markers for $transactionalId; begin transition " +
      s"to ${newMetadata.txnState}")

    txnStateManager.getTransactionState(transactionalId) match {
      case Left(Errors.NOT_COORDINATOR) =>
        info(s"No longer the coordinator for $transactionalId with coordinator epoch " +
          s"$coordinatorEpoch; cancel appending $newMetadata to transaction log")

      case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
        info(s"Loading the transaction partition that contains $transactionalId while my " +
          s"current coordinator epoch is $coordinatorEpoch; so cancel appending $newMetadata to " +
          s"transaction log since the loading process will continue the remaining work")

      case Left(unexpectedError) =>
        throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

      case Right(Some(epochAndMetadata)) =>
        if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
          debug(s"Sending $transactionalId's transaction markers for $txnMetadata with " +
            s"coordinator epoch $coordinatorEpoch succeeded, trying to append complete transaction log now")
          tryAppendToLog(PendingCompleteTxn(transactionalId, coordinatorEpoch, txnMetadata, newMetadata))
        } else {
          info(s"The cached metadata $txnMetadata has changed to $epochAndMetadata after " +
            s"completed sending the markers with coordinator epoch $coordinatorEpoch; abort " +
            s"transiting the metadata to $newMetadata as it may have been updated by another process")
        }

      case Right(None) =>
        val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, " +
          s"but there is no metadata in the cache; this is not expected"
        fatal(errorMsg)
        throw new IllegalStateException(errorMsg)
    }
  }

  def addTxnMarkersToSend(coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata,
                          newMetadata: TxnTransitMetadata): Unit = {
    val transactionalId = txnMetadata.transactionalId
    val pendingCompleteTxn = PendingCompleteTxn(
      transactionalId,
      coordinatorEpoch,
      txnMetadata,
      newMetadata)

    val prev = transactionsWithPendingMarkers.put(transactionalId, pendingCompleteTxn)
    if (prev != null) {
      info(s"Replaced an existing pending complete txn $prev with $pendingCompleteTxn while adding markers to send.")
    }
    addTxnMarkersToBrokerQueue(txnMetadata.producerId,
      txnMetadata.producerEpoch, txnResult, pendingCompleteTxn, txnMetadata.topicPartitions.toSet)
    maybeWriteTxnCompletion(transactionalId)
  }

  def numTxnsWithPendingMarkers: Int = transactionsWithPendingMarkers.size

  private def hasPendingMarkersToWrite(txnMetadata: TransactionMetadata): Boolean = {
    txnMetadata.inLock {
      txnMetadata.topicPartitions.nonEmpty
    }
  }

  def maybeWriteTxnCompletion(transactionalId: String): Unit = {
    Option(transactionsWithPendingMarkers.get(transactionalId)).foreach { pendingCompleteTxn =>
      if (!hasPendingMarkersToWrite(pendingCompleteTxn.txnMetadata) &&
          transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)) {
        writeTxnCompletion(pendingCompleteTxn)
      }
    }
  }

  private def tryAppendToLog(txnLogAppend: PendingCompleteTxn): Unit = {
    // try to append to the transaction log
    def appendCallback(error: Errors): Unit =
      error match {
        case Errors.NONE =>
          trace(s"Completed transaction for ${txnLogAppend.transactionalId} with coordinator epoch ${txnLogAppend.coordinatorEpoch}, final state after commit: ${txnLogAppend.txnMetadata.state}")

        case Errors.NOT_COORDINATOR =>
          info(s"No longer the coordinator for transactionalId: ${txnLogAppend.transactionalId} while trying to append to transaction log, skip writing to transaction log")

        case Errors.COORDINATOR_NOT_AVAILABLE =>
          info(s"Not available to append $txnLogAppend: possible causes include ${Errors.UNKNOWN_TOPIC_OR_PARTITION}, ${Errors.NOT_ENOUGH_REPLICAS}, " +
            s"${Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND} and ${Errors.REQUEST_TIMED_OUT}; retry appending")

          // enqueue for retry
          txnLogAppendRetryQueue.add(txnLogAppend)

        case Errors.COORDINATOR_LOAD_IN_PROGRESS =>
          info(s"Coordinator is loading the partition ${txnStateManager.partitionFor(txnLogAppend.transactionalId)} and hence cannot complete append of $txnLogAppend; " +
            s"skip writing to transaction log as the loading process should complete it")

        case other: Errors =>
          val errorMsg = s"Unexpected error ${other.exceptionName} while appending to transaction log for ${txnLogAppend.transactionalId}"
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }

    txnStateManager.appendTransactionToLog(txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch,
      txnLogAppend.newMetadata, appendCallback, _ == Errors.COORDINATOR_NOT_AVAILABLE, RequestLocal.NoCaching)
  }

  def addTxnMarkersToBrokerQueue(producerId: Long,
                                 producerEpoch: Short,
                                 result: TransactionResult,
                                 pendingCompleteTxn: PendingCompleteTxn,
                                 topicPartitions: immutable.Set[TopicPartition]): Unit = {
    // 通过事务id的绝对值hashcode来对事务主题的总分区数进行求余，得到事务对应的分区
    val txnTopicPartition = txnStateManager.partitionFor(pendingCompleteTxn.transactionalId)
    // 按照领导者节点进行分组
    val partitionsByDestination: immutable.Map[Option[Node], immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName)
    }

    val coordinatorEpoch = pendingCompleteTxn.coordinatorEpoch
    for ((broker: Option[Node], topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      broker match {
        //If the leader is known, and the listener name is available, return Some(node).
        case Some(brokerNode) =>
          val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
          val pendingCompleteTxnAndMarker = PendingCompleteTxnAndMarkerEntry(pendingCompleteTxn, marker)

          if (brokerNode == Node.noNode) {
            // if the leader of the partition is known but node not available, put it into an unknown broker queue
            // and let the sender thread to look for its broker and migrate(迁移) them later
            markersQueueForUnknownBroker.addMarkers(txnTopicPartition, pendingCompleteTxnAndMarker)
          } else {
            addMarkersForBroker(brokerNode, txnTopicPartition, pendingCompleteTxnAndMarker)
          }

        case None => // if the leader is not known, return None
          val transactionalId = pendingCompleteTxn.transactionalId
          txnStateManager.getTransactionState(transactionalId) match {
            case Left(error) =>
              info(s"Encountered $error trying to fetch transaction metadata for $transactionalId with coordinator epoch $coordinatorEpoch; cancel sending markers to its partition leaders")
              transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                info(s"The cached metadata has changed to $epochAndMetadata (old coordinator epoch is $coordinatorEpoch) since preparing to send markers; cancel sending markers to its partition leaders")
                transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)
              } else {
                // if the leader of the partition is unknown, skip sending the txn marker since
                // the partition is likely to be deleted already
                info(s"Couldn't find leader endpoint for partitions $topicPartitions while trying to send transaction markers for " +
                  s"$transactionalId, these partitions are likely deleted already and hence(因此) can be skipped")

                val txnMetadata = epochAndMetadata.transactionMetadata

                txnMetadata.inLock {
                  topicPartitions.foreach(txnMetadata.removePartition)
                }

                maybeWriteTxnCompletion(transactionalId)
              }

            case Right(None) =>
              val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected"
              fatal(errorMsg)
              throw new IllegalStateException(errorMsg)

          }
      }
    }

    wakeup()
  }

  def removeMarkersForTxnTopicPartition(txnTopicPartitionId: Int): Unit = {
    markersQueueForUnknownBroker.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
      for (entry <- queue.asScala) {
        info(s"Removing $entry for txn partition $txnTopicPartitionId to destination broker -1")
        removeMarkersForTxn(entry.pendingCompleteTxn)
      }
    }

    markersQueuePerBroker.foreach { case(brokerId, brokerQueue) =>
      brokerQueue.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
        for (entry <- queue.asScala) {
          info(s"Removing $entry for txn partition $txnTopicPartitionId to destination broker $brokerId")
          removeMarkersForTxn(entry.pendingCompleteTxn)
        }
      }
    }
  }

  def removeMarkersForTxn(pendingCompleteTxn: PendingCompleteTxn): Unit = {
    val transactionalId = pendingCompleteTxn.transactionalId
    val removed = transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)
    if (!removed) {
      val current = transactionsWithPendingMarkers.get(transactionalId)
      if (current != null) {
        info(s"Failed to remove pending marker entry $current trying to remove $pendingCompleteTxn")
      }
    }
  }
}

case class PendingCompleteTxn(transactionalId: String,
                              coordinatorEpoch: Int,
                              txnMetadata: TransactionMetadata,
                              newMetadata: TxnTransitMetadata) {

  override def toString: String = {
    "PendingCompleteTxn(" +
      s"transactionalId=$transactionalId, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"txnMetadata=$txnMetadata, " +
      s"newMetadata=$newMetadata)"
  }
}

case class PendingCompleteTxnAndMarkerEntry(pendingCompleteTxn: PendingCompleteTxn, txnMarkerEntry: TxnMarkerEntry)
