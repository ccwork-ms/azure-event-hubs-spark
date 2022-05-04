/*
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

package org.apache.spark.eventhubs.client

import java.time.Duration
import java.util.concurrent._

import com.microsoft.azure.eventhubs._
import org.apache.spark.SparkEnv
import org.apache.spark.eventhubs.utils.MetricPlugin
import org.apache.spark.eventhubs.utils.RetryUtils.{ after, retryJava, retryNotNull }
import org.apache.spark.eventhubs.{
  DefaultConsumerGroup,
  EventHubsConf,
  EventHubsUtils,
  NameAndPartition,
  SequenceNumber,
  PartitionPerformanceReceiver
}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, Future, Promise }

private[client] class CachedReceivedData (startSeqNo: SequenceNumber,
                                          batchSize: Int,
                                          cachedData: Seq[EventData]) {

    def matchSeqNoAndBatchSize(reqStartSeqNo: SequenceNumber, reqBatchSize: Int): Boolean = {
      if ((startSeqNo == reqStartSeqNo) && (batchSize == reqBatchSize)) {
        return true
      }
        false
    }

    def getCachedDataIterator(): Iterator[EventData] = {
      cachedData.iterator
    }
}

private[spark] trait CachedReceiver {
  private[eventhubs] def receive(ehConf: EventHubsConf,
                                 nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int): Iterator[EventData]
}

/**
 * An Event Hubs receiver instance that is cached on a Spark Executor to be
 * reused across batches. In Structured Streaming and Spark Streaming,
 * partitions are generated on the same executors across batches, so that
 * receivers can be cached are reused for maximum efficiency. Receiver caching
 * allows the underlying [[PartitionReceiver]]s to prefetch [[EventData]] from
 * the service before DataFrames or RDDs are generated by Spark.
 *
 * This class creates and maintains an AMQP link with the Event Hubs service.
 * On creation, an [[EventHubClient]] is borrowed from the [[ClientConnectionPool]].
 * Then a [[PartitionReceiver]] is created on top of the borrowed connection with the
 * [[NameAndPartition]].
 *
 * @param ehConf the [[EventHubsConf]] which contains the connection string used to connect to Event Hubs
 * @param nAndP  the Event Hub name and partition that the receiver is connected to.
 */
private[client] class CachedEventHubsReceiver private (ehConf: EventHubsConf,
                                                       nAndP: NameAndPartition,
                                                       startSeqNo: SequenceNumber)
    extends Logging {

  type AwaitTimeoutException = java.util.concurrent.TimeoutException

  import org.apache.spark.eventhubs._

  private lazy val namespaceUri: String = ehConf.namespaceUri
  private lazy val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)

  private lazy val metricPlugin: Option[MetricPlugin] = ehConf.metricPlugin()

  private lazy val client: EventHubClient = ClientConnectionPool.borrowClient(ehConf)

  private var receiver: PartitionReceiver = createReceiver(startSeqNo)

  private var cachedData: CachedReceivedData = new CachedReceivedData(-1, -1, null)

  private def createReceiver(seqNo: SequenceNumber): PartitionReceiver = {
    val taskId = EventHubsUtils.getTaskId
    logInfo(
      s"(TID $taskId) creating receiver for namespaceUri: $namespaceUri EventHubNameAndPartition: $nAndP " +
        s"consumer group: $consumerGroup. seqNo: $seqNo")
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(true)
    receiverOptions.setPrefetchCount(ehConf.prefetchCount.getOrElse(DefaultPrefetchCount))
    receiverOptions.setIdentifier(s"spark-${SparkEnv.get.executorId}-$taskId")
    val consumer = retryJava(
      EventHubsUtils.createReceiverInner(client,
                                         ehConf.useExclusiveReceiver,
                                         consumerGroup,
                                         nAndP.partitionId.toString,
                                         EventPosition.fromSequenceNumber(seqNo).convert,
                                         receiverOptions),
      "CachedReceiver creation."
    )
    Await.result(consumer, ehConf.internalOperationTimeout)
  }

  private def lastReceivedOffset(): Future[Long] = {
    if (receiver.getEventPosition.getSequenceNumber != null) {
      Future.successful(receiver.getEventPosition.getSequenceNumber)
    } else {
      Future.successful(-1)
    }
  }

  private def receiveOne(timeout: Duration, msg: String): Future[Iterable[EventData]] = {
    def receiveOneWithRetry(timeout: Duration,
                            msg: String,
                            retryCount: Int): Future[Iterable[EventData]] = {
      if (!receiver.getIsOpen && retryCount < RetryCount) {
        val taskId = EventHubsUtils.getTaskId
        logInfo(
          s"(TID $taskId) receiver is not opened yet. Will retry {$retryCount} for namespaceUri: $namespaceUri " +
            s"EventHubNameAndPartition: $nAndP consumer group: $consumerGroup")

        val retry = retryCount + 1
        after(WaitInterval.milliseconds)(receiveOneWithRetry(timeout, msg, retry))
      } else {
        receiver.setReceiveTimeout(timeout)
        retryNotNull(receiver.receive(1), msg).map(
          _.asScala
        )
      }
    }
    receiveOneWithRetry(timeout, msg, 0)
  }

  private def closeReceiver(): Future[Void] = {
    // Closing a PartitionReceiver is not a retryable operation: after the first call, it always
    // returns the same CompletableFuture. Therefore, if it fails with a transient
    // error, log and continue.
    // val dummyResult = Future[Void](null)
    val dummyResult = Promise[Void]()
    dummyResult success null
    retryJava(receiver.close(), "closing a receiver", replaceTransientErrors = dummyResult.future)
  }

  private def recreateReceiver(seqNo: SequenceNumber): Unit = {
    val taskId = EventHubsUtils.getTaskId
    val startTimeNs = System.nanoTime()
    def elapsedTimeNs = System.nanoTime() - startTimeNs

    if (!ehConf.useExclusiveReceiver) {
      Await.result(closeReceiver(), ehConf.internalOperationTimeout)
    }

    receiver = createReceiver(seqNo)

    val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
    logInfo(s"(TID $taskId) Finished recreating a receiver for namespaceUri: $namespaceUri EventHubNameAndPartition: " +
      s"$nAndP consumer group: $consumerGroup: $elapsedTimeMs ms")
  }

  private def checkCursor(requestSeqNo: SequenceNumber): Future[Iterable[EventData]] = {
    val taskId = EventHubsUtils.getTaskId

    val lastReceivedSeqNo =
      Await.result(lastReceivedOffset(), ehConf.internalOperationTimeout)

    if ((lastReceivedSeqNo > -1 && lastReceivedSeqNo + 1 != requestSeqNo) ||
        !receiver.getIsOpen) {
      logInfo(s"(TID $taskId) checkCursor. Recreating a receiver for namespaceUri: $namespaceUri " +
        s"EventHubNameAndPartition: $nAndP consumer group: $consumerGroup. requestSeqNo: $requestSeqNo, " +
        s"lastReceivedSeqNo: $lastReceivedSeqNo, isOpen: ${receiver.getIsOpen}")

      recreateReceiver(requestSeqNo)
    }

    val event = awaitReceiveMessage(
      receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor initial"),
      requestSeqNo)
    val receivedSeqNo = event.head.getSystemProperties.getSequenceNumber

    if (receivedSeqNo != requestSeqNo) {
      // This can happen in two cases:
      // 1) Your desired event is still in the service, but the receiver
      //    cursor is in the wrong spot.
      // 2) Your desired event has expired from the service.
      // First, we'll check for case (1).

      logInfo(
        s"(TID $taskId) checkCursor. Recreating a receiver for namespaceUri: $namespaceUri EventHubNameAndPartition:" +
          s" $nAndP consumer group: $consumerGroup. requestSeqNo: $requestSeqNo, receivedSeqNo: $receivedSeqNo")
      recreateReceiver(requestSeqNo)
      val movedEvent = awaitReceiveMessage(
        receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor move"),
        requestSeqNo)
      val movedSeqNo = movedEvent.head.getSystemProperties.getSequenceNumber
      if (movedSeqNo != requestSeqNo) {
        // The event still isn't present. It must be (2).
        val info = Await.result(
          retryJava(client.getPartitionRuntimeInformation(nAndP.partitionId.toString),
                    "partitionRuntime"),
          ehConf.internalOperationTimeout)

        if (requestSeqNo < info.getBeginSequenceNumber &&
            movedSeqNo == info.getBeginSequenceNumber) {
          Future {
            movedEvent
          }
        } else {
          throw new IllegalStateException(
            s"In partition ${info.getPartitionId} of ${info.getEventHubPath}, with consumer group $consumerGroup, " +
              s"request seqNo $requestSeqNo is less than the received seqNo $receivedSeqNo. The earliest seqNo is " +
              s"${info.getBeginSequenceNumber}, the last seqNo is ${info.getLastEnqueuedSequenceNumber}, and " +
              s"received seqNo $movedSeqNo")
        }
      } else {
        Future {
          movedEvent
        }
      }
    } else {
      Future {
        event
      }
    }
  }

  private def receive(requestSeqNo: SequenceNumber, batchSize: Int): Iterator[EventData] = {
    val taskId = EventHubsUtils.getTaskId
    val startTimeNs = System.nanoTime()
    def elapsedTimeNs = System.nanoTime() - startTimeNs

    // first check if this call re-executes a stream that has already been received.
    // This situation could happen if multiple actions or writers are using the
    // same stream. In this case, we return the cached data.
    if (cachedData.matchSeqNoAndBatchSize(requestSeqNo, batchSize)) {
      logInfo(s"(TID $taskId) Returned data from cache for namespaceUri: $namespaceUri EventHubNameAndPartition: $nAndP " +
        s"consumer group: $consumerGroup, requestSeqNo: $requestSeqNo, batchSize: $batchSize")
      return cachedData.getCachedDataIterator()
    }

    // Retrieve the events. First, we get the first event in the batch.
    // Then, if the succeeds, we collect the rest of the data.
    val first = Await.result(checkCursor(requestSeqNo), ehConf.internalOperationTimeout)
    val firstSeqNo = first.head.getSystemProperties.getSequenceNumber
    val batchCount = (requestSeqNo + batchSize - firstSeqNo).toInt

    if (batchCount <= 0) {
      return Iterator.empty
    }

    val theRest = for { i <- 1 until batchCount } yield
      awaitReceiveMessage(receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout),
                                     s"receive; $nAndP; seqNo: ${requestSeqNo + i}"),
                          requestSeqNo)
    // Combine and sort the data.
    val combined = first ++ theRest.flatten
    val sortedSeq = combined.toSeq
      .sortWith((e1, e2) =>
        e1.getSystemProperties.getSequenceNumber < e2.getSystemProperties.getSequenceNumber)

    cachedData = new CachedReceivedData(requestSeqNo, batchSize, sortedSeq)

    val sorted = sortedSeq.iterator
    val (result, validate) = sorted.duplicate
    val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)

    // if slowPartitionAdjustment is on, send the partition performance for this batch to the driver
    if (ehConf.slowPartitionAdjustment) {
      sendPartitionPerformanceToDriver(
        PartitionPerformanceMetric(nAndP,
                                   EventHubsUtils.getTaskContextSlim,
                                   requestSeqNo,
                                   batchCount,
                                   elapsedTimeMs))
    }

    if (metricPlugin.isDefined) {
      val (validateSize, batchSizeInBytes) =
        validate
          .map(eventData => (1, eventData.getBytes.length.toLong))
          .reduceOption { (countAndSize1, countAndSize2) =>
            (countAndSize1._1 + countAndSize2._1, countAndSize1._2 + countAndSize2._2)
          }
          .getOrElse((0, 0L))
      metricPlugin.foreach(
        _.onReceiveMetric(EventHubsUtils.getTaskContextSlim,
                          nAndP,
                          batchCount,
                          batchSizeInBytes,
                          elapsedTimeMs))
      assert(validateSize == batchCount)
    } else {
      assert(validate.size == batchCount)
    }
    logInfo(s"(TID $taskId) Finished receiving for namespaceUri: $namespaceUri EventHubNameAndPartition: $nAndP " +
      s"consumer group: $consumerGroup, batchSize: $batchSize, elapsed time: $elapsedTimeMs ms")
    result
  }

  private def awaitReceiveMessage[T](awaitable: Awaitable[T], requestSeqNo: SequenceNumber): T = {
    val taskId = EventHubsUtils.getTaskId

    try {
      Await.result(awaitable, ehConf.internalOperationTimeout)
    } catch {
      case e: AwaitTimeoutException =>
        logError(
          s"(TID $taskId) awaitReceiveMessage call failed with timeout. NamespaceUri: $namespaceUri " +
            s"EventHubNameAndPartition: $nAndP consumer group: $consumerGroup. requestSeqNo: $requestSeqNo")

        recreateReceiver(requestSeqNo)
        throw e
    }
  }

  // send the partition perforamcne metric (elapsed time for receiving events in the batch) to the
  // driver without waiting for any response.
  private def sendPartitionPerformanceToDriver(partitionPerformance: PartitionPerformanceMetric) = {
    logDebug(
      s"(Task: ${EventHubsUtils.getTaskContextSlim}) sends PartitionPerformanceMetric: " +
        s"${partitionPerformance} to the driver.")
    try {
      CachedEventHubsReceiver.partitionPerformanceReceiverRef.send(partitionPerformance)
    } catch {
      case e: Exception =>
        logError(
          s"(Task: ${EventHubsUtils.getTaskContextSlim}) failed to send the RPC message containing " +
            s"PartitionPerformanceMetric: ${partitionPerformance} to the driver with error: ${e}.")
    }
  }
}

/**
 * A companion object to the [[CachedEventHubsReceiver]]. This companion object
 * serves as a singleton which carries all the cached receivers on a given
 * Spark executor.
 */
private[spark] object CachedEventHubsReceiver extends CachedReceiver with Logging {

  private val startRecieverTimeNs = System.nanoTime()

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val receivers = new MutableMap[String, CachedEventHubsReceiver]()

  // RPC endpoint for partition performance communication in the executor
  val partitionPerformanceReceiverRef =
    RpcUtils.makeDriverRef(PartitionPerformanceReceiver.ENDPOINT_NAME,
                           SparkEnv.get.conf,
                           SparkEnv.get.rpcEnv)

  private def key(ehConf: EventHubsConf, nAndP: NameAndPartition): String = {
    (ehConf.connectionString + ehConf.consumerGroup + nAndP.partitionId).toLowerCase
  }

  private[eventhubs] override def receive(ehConf: EventHubsConf,
                                          nAndP: NameAndPartition,
                                          requestSeqNo: SequenceNumber,
                                          batchSize: Int): Iterator[EventData] = {
    val taskId = EventHubsUtils.getTaskId

    logInfo(s"(TID $taskId) EventHubsCachedReceiver look up. For namespaceUri ${ehConf.namespaceUri} " +
      s"EventHubNameAndPartition $nAndP consumer group ${ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)}. " +
      s"requestSeqNo: $requestSeqNo, batchSize: $batchSize")
    var receiver: CachedEventHubsReceiver = null
    receivers.synchronized {
      receiver = receivers.getOrElseUpdate(key(ehConf, nAndP), {
        CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo)
      })
    }
    try {
      receiver.receive(requestSeqNo, batchSize)
    } catch {
      case completionExecution: CompletionException =>
        val exceptionCause = completionExecution.getCause
        if (exceptionCause != null &&  exceptionCause.isInstanceOf[RejectedExecutionException] && exceptionCause.getMessage.contains("ReactorDispatcher instance is closed")) {
          // reactor dispatcher closed case
          logInfo(s"(TID $taskId) EventHubsCachedReceiver receive execution for namespaceUri ${ehConf.namespaceUri} " +
            s"EventHubNameAndPartition $nAndP consumer group ${ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)} " +
            s"failed with $completionExecution. Try to recreate the entire CachedEventHubsReceiver instance in order to " +
            s"use a fresh EventHubClient from the underlying java SDK, then try receiving events again.")
          receiver.client.close();
          receiver = CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo)
          receivers.synchronized {
            receivers.update(key(ehConf, nAndP), receiver)
          }
          receiver.receive(requestSeqNo, batchSize)
        } else if (exceptionCause != null &&  exceptionCause.isInstanceOf[ReceiverDisconnectedException]) {
          logInfo(s"(TID $taskId) EventHubsCachedReceiver receive execution for namespaceUri ${ehConf.namespaceUri} " +
            s"EventHubNameAndPartition $nAndP consumer group ${ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)} " +
            s"failed because another receiver for the same <NS-EH-CG-Part> combo has been created and caused this one " +
            s"to get disconnected. The full error is: $completionExecution. Throw the exception so that the driver can " +
            s"retry the task.")
          throw completionExecution
        } else {
          throw completionExecution
        }

    }
  }

  def apply(ehConf: EventHubsConf,
            nAndP: NameAndPartition,
            startSeqNo: SequenceNumber): CachedEventHubsReceiver = {
    new CachedEventHubsReceiver(ehConf, nAndP, startSeqNo)
  }
}
