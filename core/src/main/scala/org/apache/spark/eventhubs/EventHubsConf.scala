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

package org.apache.spark.eventhubs

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.net.URI

import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider.AuthenticationCallback
import org.apache.spark.eventhubs.PartitionPreferredLocationStrategy.PartitionPreferredLocationStrategy
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.eventhubs.utils.{
  AadAuthenticationCallback,
  MetricPlugin,
  ThrottlingStatusPlugin
}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
 * Configuration for your EventHubs instance when being used with Apache Spark.
 *
 * Namespace, name, keyName, key, and consumerGroup are required.
 *
 * EventHubsConf is case insensitive.
 *
 * You can start from the beginning of a stream, end of a stream, from particular offsets, or from
 * particular enqueue times. If none of those are provided, we will start from the beginning of your stream.
 * If more than one of those are provided, you will get a runtime error.
 *
 * @param connectionStr a valid connection string which will be used to connect to
 *                      an EventHubs instance. A connection string can be obtained from
 *                      the Azure portal or by using [[ConnectionStringBuilder]].
 */
final class EventHubsConf private (private val connectionStr: String)
    extends Serializable
    with Logging
    with Cloneable {
  self =>

  import EventHubsConf._

  private val settings = new ConcurrentHashMap[String, String]()
  this.setConnectionString(connectionStr)

  private[eventhubs] def set[T](key: String, value: T): EventHubsConf = {
    if (key == null) {
      throw new NullPointerException("set: null key")
    }
    if (value == null) {
      throw new NullPointerException(s"set: null value for $key")
    }

    val lowerKey = key.toLowerCase

    if (self.get(lowerKey).isDefined && lowerKey != ConnectionStringKey.toLowerCase) {
      logWarning(s"$key has already been set to ${self.get(key).get}. Overwriting with $value")
    }

    settings.put(lowerKey, value.toString)
    this
  }

  private def remove(key: String): EventHubsConf = {
    settings.remove(key)
    this
  }

  private[spark] def apply(key: String): String = {
    get(key).get
  }

  private def get(key: String): Option[String] = {
    Option(settings.get(key.toLowerCase))
  }

  /**
   * Converts the [[EventHubsConf]] instance to a [[Map[String,String]]].
   *
   * @return a [[Map[String,String]] with all the current settings in the [[EventHubsConf]]
   */
  def toMap: Map[String, String] = {
    CaseInsensitiveMap(settings.asScala.toMap)
  }

  /**
   * Clones your [[EventHubsConf]] instance.
   *
   * @return a copy of your [[EventHubsConf]]
   */
  override def clone: EventHubsConf = {
    val newConf = EventHubsConf(self.connectionString)
    newConf.settings.putAll(self.settings)
    newConf
  }

  /**
   * Indicates if some EventHubsConf is equal to this one.
   *
   * @param obj the object being compared
   * @return true if they are equal; otherwise, return false
   */
  override def equals(obj: Any): Boolean =
    obj match {
      case that: EventHubsConf => self.settings.equals(that.settings)
      case _                   => false
    }

  /**
   * Sets the connection string which will be used to connect to
   * an EventHubs instance. Connection strings can be obtained from
   * the Azure portal or using the [[ConnectionStringBuilder]].
   *
   * @param connectionString a valid connection string
   * @return the updated [[EventHubsConf]] instance
   */
  def setConnectionString(connectionString: String): EventHubsConf = {
    set(ConnectionStringKey, EventHubsUtils.encrypt(connectionString))
  }

  /** The currently set connection string */
  def connectionString: String = {
    EventHubsUtils.decrypt(self.get(ConnectionStringKey).get)
  }

  /**
   * Sets the name of your EventHub instance. The connection string used to
   * construct the [[EventHubsConf]] is updated as well.
   *
   * @param name the name of an EventHub instance
   * @return the updated [[EventHubsConf]] instance
   */
  def setName(name: String): EventHubsConf = {
    val newConnStr = ConnectionStringBuilder(connectionString).setEventHubName(name).toString
    setConnectionString(newConnStr)
  }

  /**
   * This method removes all parameters that aren't needed by Spark executors.
   *
   * @return trimmed [[EventHubsConf]]
   */
  private[spark] def trimmed: EventHubsConf = {
    // These are the options needed by Spark executors
    val include = Seq(
      ConnectionStringKey,
      ConsumerGroupKey,
      ReceiverTimeoutKey,
      MaxSilentTimeKey,
      OperationTimeoutKey,
      PrefetchCountKey,
      ThreadPoolSizeKey,
      UseExclusiveReceiverKey,
      UseSimulatedClientKey,
      MetricPluginKey,
      SlowPartitionAdjustmentKey,
      ThrottlingStatusPluginKey,
      MaxAcceptableBatchReceiveTimeKey,
      UseAadAuthKey,
      AadAuthCallbackKey,
      AadAuthCallbackParamsKey,
      DynamicPartitionDiscoveryKey
    ).map(_.toLowerCase).toSet

    val trimmedConfig = EventHubsConf(connectionString)
    settings.asScala
      .filter(k => include.contains(k._1))
      .foreach(setting => trimmedConfig.set(setting._1, setting._2))

    trimmedConfig
  }

  /** The currently set EventHub name */
  def name: String = ConnectionStringBuilder(connectionString).getEventHubName

  /** The namespace uri in String*/
  def namespaceUri: String = ConnectionStringBuilder(connectionString).getEndpoint.toString

  /** Set the consumer group for your EventHubs instance. If no consumer
   * group is provided, then [[DefaultConsumerGroup]] will be used.
   *
   * @param consumerGroup the consumer group to be used
   * @return the updated [[EventHubsConf]] instance
   */
  def setConsumerGroup(consumerGroup: String): EventHubsConf = {
    set(ConsumerGroupKey, consumerGroup)
  }

  /** The currently set consumer group. */
  def consumerGroup: Option[String] = {
    self.get(ConsumerGroupKey)
  }

  /**
   * Sets the default starting position for all partitions.
   *
   * If you would like to start from a different position for a specific partition,
   * please see [[setStartingPositions()]]. If a position is set for particular partition,
   * we will use that position instead of the one set by this method.
   *
   * If no starting position is set, then [[DefaultEventPosition]] is used
   * (i.e. we will start from the beginning of the EventHub partition.)
   *
   * @param eventPosition the default position to start receiving events from.
   * @return the updated [[EventHubsConf]] instance
   * @see [[EventPosition]]
   */
  def setStartingPosition(eventPosition: EventPosition): EventHubsConf = {
    set(StartingPositionKey, EventHubsConf.write(eventPosition))
  }

  /**
   * The currently set starting position.
   *
   * @see [[EventPosition]]
   */
  def startingPosition: Option[EventPosition] = {
    self.get(StartingPositionKey) map EventHubsConf.read[EventPosition]
  }

  /**
   * Sets starting positions on a per partition basis. This takes precedent over all
   * other configurations. If nothing is set here, then we will defer to what has been set
   * in [[setStartingPosition()]]. If nothing is set in [[setStartingPosition()]], then
   * we will start consuming from the start of the EventHub partition.
   *
   * @param eventPositions a map of partition ids (ints) to [[EventPosition]]s
   * @return the updated [[EventHubsConf]] instance
   * @see [[EventPosition]]
   */
  def setStartingPositions(eventPositions: Map[NameAndPartition, EventPosition]): EventHubsConf = {
    val m = eventPositions.map { case (k, v) => k.toString -> v }
    set(StartingPositionsKey, EventHubsConf.write[Map[String, EventPosition]](m))
  }

  /**
   * The currently set positions for particular partitions.
   *
   * @see [[EventPosition]]
   */
  def startingPositions: Option[Map[NameAndPartition, EventPosition]] = {
    val m = self.get(StartingPositionsKey) map EventHubsConf
      .read[Map[String, EventPosition]] getOrElse Map.empty
    if (m.isEmpty) None else Some(m.map { case (k, v) => NameAndPartition.fromString(k) -> v })
  }

  /**
   * Sets the default ending position for all partitions. Only relevant for batch style queries
   * in Structured Streaming.
   *
   * Per partition configuration is allowed using [[setEndingPositions()]]. If nothing is set
   * in [[setEndingPositions()]], then the value set here (in [[setEndingPosition()]]) is used.
   * If nothing is set here, then we will use the default value (which is the end of the stream).
   *
   * @param eventPosition the default position to start receiving events from.
   * @return the updated [[EventHubsConf]] instance
   */
  def setEndingPosition(eventPosition: EventPosition): EventHubsConf = {
    set(EndingPositionKey, EventHubsConf.write(eventPosition))
  }

  /**
   * The currently set starting position.
   *
   * @see [[EventPosition]]
   */
  def endingPosition: Option[EventPosition] = {
    self.get(EndingPositionKey) map EventHubsConf.read[EventPosition]
  }

  /**
   * Sets ending positions on a per partition basis. This is only relevant for batch-styled
   * queries in Structured Streaming. If nothing is set here, then the position set in
   * [[setEndingPosition]] is used. If nothing is set in [[setEndingPosition]], then the default
   * value is used. The default value is the end of the stream.
   *
   * @param eventPositions a map of partition ids (ints) to [[EventPosition]]s
   * @return the updated [[EventHubsConf]] instance
   * @see [[EventPosition]]
   */
  def setEndingPositions(eventPositions: Map[NameAndPartition, EventPosition]): EventHubsConf = {
    val m = eventPositions.map { case (k, v) => k.toString -> v }
    set(EndingPositionsKey, EventHubsConf.write[Map[String, EventPosition]](m))
  }

  /**
   * the currently set positions for particular partitions.
   *
   * @see [[EventPosition]]
   */
  def endingPositions: Option[Map[NameAndPartition, EventPosition]] = {
    val m = self.get(EndingPositionsKey) map EventHubsConf
      .read[Map[String, EventPosition]] getOrElse Map.empty
    if (m.isEmpty) None else Some(m.map { case (k, v) => NameAndPartition.fromString(k) -> v })
  }

  /**
   * maxRatePerPartition defines an upper bound for how many events will be
   * in a partition per batch. This method sets a max rate per partition for
   * all partitions.
   *
   * If you would like to set a different max rate for a particular partition,
   * then see [[setMaxRatesPerPartition()]]. If a max rate per partition has been set
   * using [[setMaxRatesPerPartition()]], that rate will be used. If nothing has been
   * set, then the max rate set by this method will be used. If nothing has been
   * set by the user, then we will use [[DefaultMaxRatePerPartition]].
   *
   * @param rate the default maxRatePerPartition for all partitions.
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxRatePerPartition(rate: Rate): EventHubsConf = {
    set(MaxRatePerPartitionKey, rate)
  }

  /** The currently set max rate per partition.  */
  def maxRatePerPartition: Option[Rate] = {
    self.get(MaxRatePerPartitionKey) map (_.toRate)
  }

  /**
   * For information on maxRatePerPartition, see [[setMaxRatePerPartition()]].
   *
   * This method allows users to set max rates per partition on a per partition basis.
   * If a maxRatePerPartition is set here, it will be used. If one isn't set, then the value
   * set in [[setMaxRatePerPartition()]] will be used. If no value has been set in either
   * setter method, then we will use [[DefaultMaxRatePerPartition]].
   *
   * @param rates a map of partition ids (ints) to their desired rates.
   * @return
   */
  def setMaxRatesPerPartition(rates: Map[NameAndPartition, Rate]): EventHubsConf = {
    val m = rates.map { case (k, v) => k.toString -> v }
    set(MaxRatesPerPartitionKey, EventHubsConf.write[Map[String, Rate]](m))
  }

  /** A map of partition/max rate pairs that have been set by the user.  */
  def maxRatesPerPartition: Option[Map[NameAndPartition, Rate]] = {
    val m = self.get(MaxRatesPerPartitionKey) map EventHubsConf
      .read[Map[String, Rate]] getOrElse Map.empty
    if (m.isEmpty) None else Some(m.map { case (k, v) => NameAndPartition.fromString(k) -> v })
  }

  /**
   * Set the receiver timeout. We will try to receive the expected batch for
   * the length of this timeout.
   * Default: [[DefaultReceiverTimeout]]
   *
   * @param d the new receiver timeout
   * @return the updated [[EventHubsConf]] instance
   */
  def setReceiverTimeout(d: Duration): EventHubsConf = {
    if (d.toMillis > operationTimeout.getOrElse(DefaultOperationTimeout).toMillis) {
      throw new IllegalArgumentException("receiver timeout is greater than operation timeout")
    }

    set(ReceiverTimeoutKey, d)
  }

  /** The current receiver timeout.  */
  def receiverTimeout: Option[Duration] = {
    self.get(ReceiverTimeoutKey) map (str => Duration.parse(str))
  }

  /**
   * Set the maximum silent time for a receiver. We will try to recreate the receiver
   * if there is no activity for the length of this duration.
   * Default: [[DefaultMaxSilentTime]]
   *
   * @param d the new maximum silent time
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxSilentTime(d: Duration): EventHubsConf = {
    if (d.toMillis < MinSilentTime.toMillis) {
      throw new IllegalArgumentException("max silent time is less than " + MinSilentTime)
    }

    set(MaxSilentTimeKey, d)
  }

  /** The current maximum silent time.  */
  def maxSilentTime: Option[Duration] = {
    self.get(MaxSilentTimeKey) map (str => Duration.parse(str))
  }

  /**
   * Set the operation timeout. We will retryJava failures when contacting the
   * EventHubs service for the length of this timeout.
   * Default: [[DefaultOperationTimeout]]
   *
   * @param d the new operation timeout
   * @return the updated [[EventHubsConf]] instance
   */
  def setOperationTimeout(d: Duration): EventHubsConf = {
    if (d.toMillis < receiverTimeout.getOrElse(DefaultReceiverTimeout).toMillis) {
      throw new IllegalArgumentException("operation timeout is less than receiver timeout")
    }

    set(OperationTimeoutKey, d)
  }

  /** The current operation timeout. */
  def operationTimeout: Option[Duration] = {
    self.get(OperationTimeoutKey) map (str => Duration.parse(str))
  }

  /** This is the exact same thing as operationTimeout, the only difference is in the types
   * (Java Duration vs Scala FiniteDuration)
   */
  private[spark] lazy val internalOperationTimeout: FiniteDuration = {
    scala.concurrent.duration.Duration
      .fromNanos(operationTimeout.getOrElse(DefaultOperationTimeout).toNanos)
  }

  /**
   * Set the prefetch count for the underlying receiver.
   * PrefetchCount controls how many events are received in advance.
   * Default: [[DefaultPrefetchCount]]
   *
   * @param count the new prefetch count
   * @return the updated [[EventHubsConf]] instance
   */
  def setPrefetchCount(count: Int): EventHubsConf = {
    if (count > PrefetchCountMaximum || count < PrefetchCountMinimum) {
      throw new IllegalArgumentException(s"setPrefetchCount: count value is out of range. PrefetchCount should be " +
        s"within the range [$PrefetchCountMinimum, $PrefetchCountMaximum].")
    }

    set(PrefetchCountKey, count)
  }

  /** The current prefetch count.  */
  def prefetchCount: Option[Int] = {
    self.get(PrefetchCountKey) map (str => str.toInt)
  }

  /**
   * Set the size of thread pool.
   * Default: [[DefaultThreadPoolSize]]
   *
   * @param size the size of thread pool
   * @return the updated [[EventHubsConf]] instance
   */
  def setThreadPoolSize(size: Int): EventHubsConf = {
    set(ThreadPoolSizeKey, size)
  }

  /** The current thread pool size.  */
  def threadPoolSize: Option[Int] = {
    self.get(ThreadPoolSizeKey) map (str => str.toInt)
  }

  /**
   * Rate limit on maximum number of events processed per trigger interval.
   * Only valid for Structured Streaming. The specified total number of events
   * will be proportionally split across partitions of different volume.
   *
   * @param limit the maximum number of events to be processed per trigger interval
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxEventsPerTrigger(limit: Long): EventHubsConf = {
    set(MaxEventsPerTriggerKey, limit)
  }

  def setMetricPlugin(metricPlugin: MetricPlugin): EventHubsConf = {
    set(MetricPluginKey, metricPlugin.getClass.getName)
  }

  def metricPlugin(): Option[MetricPlugin] = {
    self.get(MetricPluginKey) map (className => {
      Utils.classforName(className).newInstance().asInstanceOf[MetricPlugin]
    })
  }

  /**
   * Set the flag for slow partition adjustment. The default value is false.
   * Default: [[DefaultSlowPartitionAdjustment]]
   *
   * @param b the flag which specifies whether the connector uses slow partition adjustment logic
   * @return the updated [[EventHubsConf]] instance
   */
  def setSlowPartitionAdjustment(b: Boolean): EventHubsConf = {
    set(SlowPartitionAdjustmentKey, b)
  }

  /** The slow partition adjustment flag  */
  def slowPartitionAdjustment: Boolean = {
    self.get(SlowPartitionAdjustmentKey).getOrElse(DefaultSlowPartitionAdjustment).toBoolean
  }

  /**
   * Set the flag for dynamic partition discovery. This option is useful only if partitions are being dynamically
   * added to an existing event hub. For more information on how to dynamically add partitions to an event hub
   * please refer to [[https://docs.microsoft.com/en-us/azure/event-hubs/dynamically-add-partitions]].
   * If dynamic partition discovery is disabled the number of partitions is being read and set at the
   * beginning of the execution. Otherwise the number of partitions is being read and updated every
   * [[UpdatePartitionCountIntervalMS]] milliseconds. The default value is false.
   * Default: [[DefaultDynamicPartitionDiscovery]]
   *
   * @param b the flag which specifies whether the connector uses dynamic partition discovery
   * @return the updated [[EventHubsConf]] instance
   */
  def setDynamicPartitionDiscovery(b: Boolean): EventHubsConf = {
    set(DynamicPartitionDiscoveryKey, b)
  }

  /** The dynamic partition discovery flag  */
  def dynamicPartitionDiscovery: Boolean = {
    self.get(DynamicPartitionDiscoveryKey).getOrElse(DefaultDynamicPartitionDiscovery).toBoolean
  }

  /** Set the max time that is acceptable for a partition to receive events in a single batch.
   * This value is being used to identify slow partitions when the slowPartitionAdjustment is on.
   * Only partitions that tale more than this time to receive their portion of events in batch are considered
   * as potential slow partitions.
   * Default: [[DefaultMaxAcceptableBatchReceiveTime]]
   *
   * @param d the new maximum acceptable time for a partition to receive events in a single batch
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxAcceptableBatchReceiveTime(d: Duration): EventHubsConf = {
    set(MaxAcceptableBatchReceiveTimeKey, d)
  }

  /** The current max time that is acceptable for a partition to receive events in a single batch. */
  def maxAcceptableBatchReceiveTime: Option[Duration] = {
    self.get(MaxAcceptableBatchReceiveTimeKey) map (str => Duration.parse(str))
  }

  def setThrottlingStatusPlugin(throttlingStatusPlugin: ThrottlingStatusPlugin): EventHubsConf = {
    set(ThrottlingStatusPluginKey, throttlingStatusPlugin.getClass.getName)
  }

  def throttlingStatusPlugin(): Option[ThrottlingStatusPlugin] = {
    self.get(ThrottlingStatusPluginKey) map (className => {
      Utils.classforName(className).newInstance().asInstanceOf[ThrottlingStatusPlugin]
    })
  }

  /**
   * Set whether the connector uses an epoch receiver
   * Default: [[DefaultUseExclusiveReceiver]]
   *
   * @param b the flag which specifies whether the connector uses an epoch receiver
   * @return the updated [[EventHubsConf]] instance
   */
  def setUseExclusiveReceiver(b: Boolean): EventHubsConf = {
    set(UseExclusiveReceiverKey, b)
  }

  /** The current thread pool size.  */
  def useExclusiveReceiver: Boolean = {
    self.get(UseExclusiveReceiverKey).getOrElse(DefaultUseExclusiveReceiver).toBoolean
  }

  /**
   * Sets the partition preferred location strategy, default is to use Hash, BalancedHash also available
   * Default: [[DefaultPartitionPreferredLocationStrategy]]
   *
   * @param partitionStrategy The partition strategy to use (e.g. Hash, BalancedHash)
   * @return the updated [[EventHubsConf]] instance
   */
  def setPartitionPreferredLocationStrategy(
      partitionStrategy: PartitionPreferredLocationStrategy): EventHubsConf = {
    set(PartitionPreferredLocationStrategyKey, partitionStrategy.toString)
  }

  /** The current partitionPreferredLocationStrategy.  */
  def partitionPreferredLocationStrategy: PartitionPreferredLocationStrategy = {
    val strategyType = self
      .get(PartitionPreferredLocationStrategyKey)
      .getOrElse(DefaultPartitionPreferredLocationStrategy)
    PartitionPreferredLocationStrategy.values
      .find(_.toString == strategyType)
      .getOrElse(
        throw new IllegalStateException(
          s"Illegal partition strategy $strategyType, available types " +
            s"${PartitionPreferredLocationStrategy.values.mkString(",")}"))
  }

  /**
   * Use AAD auth to connect eventhubs instead of connection string. It's internal.
   *
   * Default: [[false]]
   * @return the updated [[EventHubsConf]] instance
   */
  private def setUseAadAuth(b: Boolean): EventHubsConf = {
    set(UseAadAuthKey, b)
  }

  def useAadAuth: Boolean = {
    self.get(UseAadAuthKey).getOrElse(DefaultUseAadAuth).toBoolean
  }

  /**
   * set a callback class for aad auth. The class should be Serializable and derived from
   * org.apache.spark.eventhubs.utils.AadAuthenticationCallback.
   * More info about this: https://docs.microsoft.com/en-us/azure/event-hubs/authorize-access-azure-active-directory
   *
   * @param callback The callback class which implements org.apache.spark.eventhubs.utils.AadAuthenticationCallback
   * @return the updated [[EventHubsConf]] instance
   */
  def setAadAuthCallback(callback: AadAuthenticationCallback): EventHubsConf = {
    setUseAadAuth(true)
    set(AadAuthCallbackKey, callback.getClass.getName)
  }

  def aadAuthCallback(): Option[AadAuthenticationCallback] = {
    val params: Map[String, Object] = self.get(AadAuthCallbackParamsKey) map EventHubsConf
      .read[Map[String, Object]] getOrElse Map.empty
    if (params.isEmpty) {
      self.get(AadAuthCallbackKey) map (className => {
        Utils
          .classforName(className)
          .getConstructor()
          .newInstance()
          .asInstanceOf[AadAuthenticationCallback]
      })
    } else {
      self.get(AadAuthCallbackKey) map (className => {
        Utils
          .classforName(className)
          .getConstructor(classOf[Map[String, Object]])
          .newInstance(params)
          .asInstanceOf[AadAuthenticationCallback]
      })
    }
  }

  /**
   * set the parameter passed to the aad auth callback class in case it accepts parameters. The aad auth class should
   * either accepts no parameters or accepts a properties bag(Map[String, Object]). This optional parameter can be used
   * to pass authentication secrets to the aad auth callback class securely.
   * More info about this: https://docs.microsoft.com/en-us/azure/event-hubs/authorize-access-azure-active-directory
   *
   * @param params The parameters passed to the aad authentication callback class. The parameters should be passed as a
   *               sequence of Strings.
   * @return the updated [[EventHubsConf]] instance
   */
  def setAadAuthCallbackParams(params: Map[String, Object]): EventHubsConf = {
    set(AadAuthCallbackParamsKey, EventHubsConf.write[Map[String, Object]](params))
  }

  // The simulated client (and simulated eventhubs) will be used. These
  // can be found in EventHubsTestUtils.
  private[spark] def setUseSimulatedClient(b: Boolean): EventHubsConf = {
    set(UseSimulatedClientKey, b)
  }

  private[spark] def useSimulatedClient: Boolean = {
    self.get(UseSimulatedClientKey).getOrElse(DefaultUseSimulatedClient).toBoolean
  }

  private[spark] def validate: Boolean = {
    if (ConnectionStringBuilder(self.connectionString).getEventHubName == null) {
      throw new IllegalStateException(
        "No EntityPath has been set in the Event Hubs connection string. The EntityPath " +
          "is simply your EventHub name. Your connection string should look like:\n" +
          "\"Endpoint=sb://SAMPLE;SharedAccessKeyName=KEY_NAME;SharedAccessKey=KEY;" +
          "EntityPath=EVENTHUB_NAME\"")
    }
    true
  }
}

object EventHubsConf extends Logging {

  private implicit val formats = Serialization.formats(NoTypeHints)

  private def read[T: Manifest](json: String): T = {
    Serialization.read[T](json)
  }

  private def write[T <: AnyRef](value: T): String = {
    Serialization.write[T](value)
  }

  // Option key values
  val ConnectionStringKey = "eventhubs.connectionString"
  val ConsumerGroupKey = "eventhubs.consumerGroup"
  val StartingPositionKey = "eventhubs.startingPosition"
  val StartingPositionsKey = "eventhubs.startingPositions"
  val EndingPositionKey = "eventhubs.endingPosition"
  val EndingPositionsKey = "eventhubs.endingPositions"
  val MaxRatePerPartitionKey = "eventhubs.maxRatePerPartition"
  val MaxRatesPerPartitionKey = "eventhubs.maxRatesPerPartition"
  val ReceiverTimeoutKey = "eventhubs.receiverTimeout"
  val MaxSilentTimeKey = "eventhubs.maxSilentTime"
  val OperationTimeoutKey = "eventhubs.operationTimeout"
  val PrefetchCountKey = "eventhubs.prefetchCount"
  val ThreadPoolSizeKey = "eventhubs.threadPoolSize"
  val UseExclusiveReceiverKey = "eventhubs.useExclusiveReceiver"
  val MaxEventsPerTriggerKey = "maxEventsPerTrigger"
  val MaxEventsPerTriggerKeyAlias = "eventhubs.maxEventsPerTrigger"
  val UseSimulatedClientKey = "useSimulatedClient"
  val MetricPluginKey = "eventhubs.metricPlugin"
  val PartitionPreferredLocationStrategyKey = "partitionPreferredLocationStrategy"
  val SlowPartitionAdjustmentKey = "eventhubs.slowPartitionAdjustment"
  val ThrottlingStatusPluginKey = "eventhubs.throttlingStatusPlugin"
  val MaxAcceptableBatchReceiveTimeKey = "eventhubs.maxAcceptableBatchReceiveTime"
  val UseAadAuthKey = "eventhubs.useAadAuth"
  val AadAuthCallbackKey = "eventhubs.aadAuthCallback"
  val AadAuthCallbackParamsKey = "eventhubs.AadAuthCallbackParams"
  val DynamicPartitionDiscoveryKey = "eventhubs.DynamicPartitionDiscovery"

  /** Creates an EventHubsConf */
  def apply(connectionString: String) = new EventHubsConf(connectionString)

  private[spark] def toConf(params: Map[String, String]): EventHubsConf = {
    val connectionString =
      EventHubsUtils.decrypt(
        params.find(_._1.toLowerCase == ConnectionStringKey.toLowerCase).get._2)

    val ehConf = EventHubsConf(connectionString)

    for ((k, v) <- params) {
      ehConf.set(k, v)
    }

    ehConf
  }
}
