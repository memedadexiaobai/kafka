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

import com.yammer.metrics.core.Timer

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  private val EventQueueTimeMetricName = "EventQueueTimeMs"
  private val EventQueueSizeMetricName = "EventQueueSize"
}

/**
 * 一个常规的事件处理机制代码：
 * ControllerEvent：需要处理的事件
 * QueuedEvent：封装了ControllerEvent，增加了事件的失效判断机制、事件开始执行的判断机制
 * ControllerEventProcessor：ControllerEvent事件处理器
 * ControllerEventManager：ControllerEvent管理器
 *  queue：封装待处理的实际集合，此处为QueuedEvent
 *  ControllerEventThread：事件处理器线程，从queue读取待处理的事件，此处为QueuedEvent
 */

trait ControllerEventProcessor {
  def process(event: ControllerEvent): Unit
  def preempt(event: ControllerEvent): Unit
}

class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  /**
   * processingStarted 是一个 CountDownLatch 对象，用于控制线程的执行。
   * countDown() 方法将计数器减 1，表示事件处理已经开始。
   * 其他线程可以使用 processingStarted.await() 等待事件处理开始
   */
  private val processingStarted = new CountDownLatch(1)
  private val spent = new AtomicBoolean(false) //spent:失效的

  def process(processor: ControllerEventProcessor): Unit = {
    /**
     * spent 是一个布尔标志，用于指示该事件是否已经被处理。
     * getAndSet(true) 将 spent 的值设置为 true，并返回其旧值。
     * 如果 spent 旧值为 true，说明事件已经被处理过，直接返回，避免重复处理
     *
     * 这段代码确保事件在控制器中按顺序且只处理一次，通过 spent 标志避免重复处理，并使用 processingStarted 通知其他线程事件处理的开始。
     */
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    processor.process(event)
  }

  //preempt：抢占
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, Timer],
                             eventQueueTimeTimeoutMs: Long = 300000) {
  import ControllerEventManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  /**
   * eventQueueTimeHist 用于统计事件队列中的事件数量，帮助代码判断是否应该使用 poll 方法进行有限时等待，还是使用 take 方法进行无限制等待。
   * 这种机制可以提高事件处理的效率和响应速度，避免不必要的阻塞和等待时间。
   */
  private val eventQueueTimeHist = metricsGroup.newHistogram(EventQueueTimeMetricName)

  metricsGroup.newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      metricsGroup.removeMetric(EventQueueTimeMetricName)
      metricsGroup.removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val preemptedEvents = new util.ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    preemptedEvents.forEach(_.preempt(processor))
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String)
    extends ShutdownableThread(
      name, false, s"[ControllerEventThread controllerId=$controllerId] ")
      with Logging {

    logIdent = logPrefix

    override def doWork(): Unit = {
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          /**
           * 更新事件在队列中的停留时间 计算事件从进入队列到被处理的历时，并更新相关的监控指标，以便对系统性能进行监控和优化。
           */
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)

            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time(() => process())
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    /**
     * eventQueueTimeHist 是一个计数器，用于统计 eventQueue 中事件的数量。在判断 count != 0 时，代码会根据事件队列中是否有事件来决定使用 poll 方法还是 take 方法从队列中获取事件。
     *
     * 如果事件队列中的事件数量不为零，调用 queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS) 尝试在指定的超时时间内获取事件。
     * 如果获取的事件为空（event == null），说明在超时时间内未能获取到事件，则调用 eventQueueTimeHist.clear() 清空事件计数器，并调用 queue.take() 从队列中获取事件，该方法会一直阻塞直到有事件可用。
     * 如果获取的事件不为空，则直接返回该事件
     *
     * 如果事件队列中的事件数量为零，直接调用 queue.take() 从队列中获取事件，该方法会一直阻塞直到有事件可用。
     *
     * 通过判断事件队列是否为空（count != 0），代码决定是尝试在超时时间内获取事件（poll）还是直接阻塞等待事件（take）
     *  如果事件队列为空，直接使用 take 方法阻塞等待新事件，避免不必要的超时等待；
     *  如果事件队列不为空，则尝试使用 poll 方法在有限时间内获取事件，如果超时则清空计数器并使用 take 方法确保最终能获取到事件。
     */
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
