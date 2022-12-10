package kafka.tools

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.io._
import java.time.Duration
import java.util.{Arrays, Collections,Properties}

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.{CommonClientConfigs,admin}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import kafka.utils.{CommandLineUtils}
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._
import scala.math._


object ConsumerLatency {
  private val timeout: Long = 60000
  private val defaultReplicationFactor: Short = 1
  private val defaultNumPartitions: Int = 1
  private val entryheadersize: Int = 70 


  def main(args: Array[String]) {
    val config = new TestConfig(args)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.consumerProps)

    val topic =  config.topic
    val messageLen = config.messageLen
    val numMessages = config.numMessages
    val warmup = config.numWarmup
    val sendTimestampFilepath = Paths.get(System.getProperty("user.dir"),"sendTimeStamps.csv").toString()  
    val receiveTimestampFilepath = Paths.get(System.getProperty("user.dir"), "receiveTimeStamps.csv").toString()

    def finalise() {
      consumer.commitSync()
      consumer.close()
    }

    // create topic if it does not exist
    if (!consumer.listTopics().containsKey(topic)) {
      try {
        createTopic(topic, config.createTopicProps)
      } catch {
        case t: Throwable =>
          finalise()
          throw new RuntimeException(s"Failed to create topic $topic", t)
      }
    }

    val topicPartitions = consumer.partitionsFor(topic).asScala
      .map(p => new TopicPartition(p.topic(), p.partition())).asJava
    consumer.assign(topicPartitions)
    consumer.seekToEnd(topicPartitions)
    consumer.assignment().asScala.foreach(consumer.position)

    var latencies = Array[Double]() 
    var sendTimes = Array.fill(numMessages + warmup)(0L)
    var receiveTimes = Array.fill(numMessages + warmup)(0L)
    var tmp = 0L
    for(l <- 0 until 10) {
        for(ind <- 0 until sendTimes.size) {
            tmp = sendTimes(ind) + receiveTimes(ind)
        }
    }

    var receivedMessages = 0
    while(receivedMessages < numMessages + warmup) {
      val records = if (config.withRdmaConsume)
                            consumer.RDMApoll(Duration.ofMillis(timeout)).asScala
                       else
                            consumer.poll(Duration.ofMillis(timeout)).asScala
      val instant = Clock.systemUTC().instant()
      val receiveTimeStamp = instant.getEpochSecond() * 1000000 + instant.getNano()/1000
      for (record <- records) {
        if (record.value != null){
          val sendTimeStamp = ByteBuffer.wrap(record.value.slice(0,8)).getLong
          if (receivedMessages < numMessages + warmup) {
            sendTimes(receivedMessages) = sendTimeStamp
            receiveTimes(receivedMessages) = receiveTimeStamp
            receivedMessages = receivedMessages + 1
          } else {
              printf("Warning: received more messages than expected. No timestamp log for those extra messages.\n")
          }
        }
      }
    }
    
    for (ind <- 0 until sendTimes.size){
      latencies :+= ( receiveTimes(ind) - sendTimes(ind) ) * 1.0
    }
    latencies = latencies.drop(config.numWarmup.toInt)
    sendTimes = sendTimes.drop(config.numWarmup.toInt)
    receiveTimes = receiveTimes.drop(config.numWarmup.toInt)

    /** -- Write Timestampes to file -- **/
    // sendTimes
    val sendTimestampfile = new File(sendTimestampFilepath)
    val sbw = new BufferedWriter(new FileWriter(sendTimestampfile))
    sbw.write(sendTimes.mkString("\n"))
    sbw.close()
    val receiveTimestampfile = new File(receiveTimestampFilepath)
    val rbw = new BufferedWriter(new FileWriter(receiveTimestampfile))
    rbw.write(receiveTimes.mkString("\n"))
    rbw.close()
    /** -- Print out of latencies and throughput -- **/
    def stdDev(arr: Array[Double], 
             mean: Double): Double = {
      var res = 0.0
      for (el <- arr){
        res = res + pow(el.toDouble - mean, 2)
      }
      res = res / arr.size
      return sqrt(res)
    }
    val average_latency = latencies.sum * 1.0 / (latencies.size )
    val std_latency = stdDev(latencies, average_latency)
    val ops = 1.0 * 1000000 * numMessages / (receiveTimes.last - sendTimes.head)
    Arrays.sort(latencies)
    printf("\n med_latency:%.1f us,avg_latency:%.1f us,std: %.1f ,ops:%.1f ,num: %d \n".format(latencies((latencies.length * 0.5).toInt),average_latency, std_latency, ops, latencies.size))
    finalise()
  }

  def createTopic(topic: String, props: Properties): Unit = {
    println("Topic \"%s\" does not exist. Will create topic with %d partition(s) and replication factor = %d"
              .format(topic, defaultNumPartitions, defaultReplicationFactor))

    val adminClient = admin.AdminClient.create(props)
    val newTopic = new NewTopic(topic, defaultNumPartitions, defaultReplicationFactor)
    try adminClient.createTopics(Collections.singleton(newTopic)).all().get()
    finally Utils.closeQuietly(adminClient, "AdminClient")
  }

  import kafka.utils.CommandDefaultOptions
  class TestConfig(args: Array[String]) extends CommandDefaultOptions(args) {

    val bootstrapServersOpt = parser.accepts("broker-list", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg()
      .describedAs("host")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send or consume")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])

    val numWarmupOpt = parser.accepts("numWarmup", "The amount of initial messages to ignore.")
      .withRequiredArg
      .describedAs("cout")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)
    
    val saveFilepathOpt = parser.accepts("saveFilepath", "The absulute filepath to save the timestamps files.")
      .withRequiredArg
      .describedAs("filepath")
      .ofType(classOf[String])
      .defaultsTo("")

    val fetchSizeOpt = parser.accepts("size", "The amount of data to produce/fetch in a single test.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)

    val withRdmaConsumeOpt = parser.accepts("with-rdma-consume", "use rdma for fetching.")

    val withSlotsOpt = parser.accepts("withslots", "use rdma slots for fetching.")

    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("consumer config file")
      .ofType(classOf[String])

    

    options = parser.parse(args: _*)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to collecty consumer latency")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt, bootstrapServersOpt,fetchSizeOpt)


    val withRdmaConsume = options.has(withRdmaConsumeOpt)

    val withrdmaslots = options.has(withSlotsOpt) //extremely important


    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties

    val topic = options.valueOf(topicOpt)
    val saveFilepath = options.valueOf(saveFilepathOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageLen = options.valueOf(fetchSizeOpt).intValue
    val numWarmup = options.valueOf(numWarmupOpt).intValue


    consumerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (messageLen + entryheadersize).toString) // for RDMA
    consumerProps.put(ConsumerConfig.WITH_SLOTS, withrdmaslots.toString) // for RDMA
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") // ensure we have no temporal batching

    val createTopicProps =  new Properties
    createTopicProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))

  }

}
