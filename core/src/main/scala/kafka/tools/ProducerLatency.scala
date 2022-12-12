package kafka.tools

import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.nio.ByteBuffer
import java.util.{Arrays,  Properties}

import org.apache.kafka.clients.{CommonClientConfigs}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{Exit, Utils}
import kafka.utils.{CommandLineUtils}

import scala.collection.JavaConverters._
import scala.util.Random
import scala.math.BigInt

object ProducerLatency {
  // From End-to-End latency
  private val timeout: Long = 60000
  private val defaultReplicationFactor: Short = 1
  private val defaultNumPartitions: Int = 1
  private val entryheadersize: Int = 70 // TODO check that is always true.


  def main(args: Array[String]) {
    val config = new TestConfig(args)

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)
    val topic =  config.topic
    val messageLen = config.messageLen
    val numMessages = config.numMessages
    val warmup = config.numWarmup
    val interval =  config.produceInterval

    def finalise() {
      producer.close()
    }
    val totalMessages = warmup + numMessages 
    var prevSendTime = 0L
    var sentMessages = 0
    var totalTime = 0.0
    val latencies = new Array[Long](totalMessages)
    val random = new Random(0)
    var firstSendTime = 0L
    var lastAckTime = 0L

    while(sentMessages < totalMessages){
      val begin = System.nanoTime
      if(begin - prevSendTime > interval){
        val message = randomBytesOfLen(random, messageLen)
        producer.RDMAsend(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
        val elapsed = System.nanoTime - begin
        prevSendTime = begin
        
        totalTime += elapsed
        latencies(sentMessages) = elapsed
        sentMessages = sentMessages + 1
        if(sentMessages==warmup){
          firstSendTime = begin
        }
        if(sentMessages == totalMessages - 1){
          lastAckTime = System.nanoTime
        }
      }
    }

    //Results
    println("Avg latency: %.4f us\n".format(totalTime / totalMessages / 1000.0 ))
    Arrays.sort(latencies)
    val p50 = latencies((latencies.length * 0.5).toInt)
    val p95 = latencies((latencies.length * 0.95).toInt)
    val p99 = latencies((latencies.length * 0.99).toInt)
    val p999 = latencies((latencies.length * 0.999).toInt)
    println("Producer latency: 50th = %f us, 95th = %f us, 99th = %f us, 99.9th = %f us".format(p50/ 1000.0, p95/ 1000.0, p99/ 1000.0, p999/ 1000.0))
    finalise()
  }

  def randomBytesOfLen(random: Random, len: Int): Array[Byte] = {
    var arr = Array.fill(len)((random.nextInt(26) + 65).toByte)
    val instant = Clock.systemUTC().instant()
    val payloadTimeStamp = instant.getEpochSecond() * 1000000 + instant.getNano()/1000
    
    var timeArr = ByteBuffer.allocate(8).putLong(payloadTimeStamp.asInstanceOf[Long]).array()
    //BigInt(payloadTimeStamp).toByteArray
    for(i <- 0 to 7){
      arr(i) = timeArr(i)
    }
    return arr
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

    val acksOpt = parser.accepts("acks", "REQUIRED: how many acks to wait.")
      .withRequiredArg
      .describedAs("acks")
      .ofType(classOf[String])

    val fetchSizeOpt = parser.accepts("size", "The amount of data to produce/fetch in a single test.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    
    val produceIntervalOpt = parser.accepts("interval", "The amount of time in nanosecond wait until producer send the next message to broker.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(1000)

    val numWarmupOpt = parser.accepts("numWarmup", "The amount of initial messages to ignore.")
      .withRequiredArg
      .describedAs("cout")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)

    val withRdmaProduceOpt = parser.accepts("with-rdma-produce", "use rdma for producing.")

    val withrdmasharedOpt = parser.accepts("with-rdma-shared", "use rdma shared access [important].")

    val producerConfigOpt = parser.accepts("producer.config", "Producer config properties file.")
      .withRequiredArg
      .describedAs("producer config file")
      .ofType(classOf[String])

    options = parser.parse(args: _*)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps in end to end latency")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt, bootstrapServersOpt,acksOpt,fetchSizeOpt)

    val withRdmaProduce = options.has(withRdmaProduceOpt)
    val withrdmashared = options.has(withrdmasharedOpt) // Extremely important!
    val acks = options.valueOf(acksOpt)
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageLen = options.valueOf(fetchSizeOpt).intValue
    val produceInterval = options.valueOf(produceIntervalOpt).longValue
    val numWarmup = options.valueOf(numWarmupOpt).intValue

    val producerProps = if (options.has(producerConfigOpt))
      Utils.loadProps(options.valueOf(producerConfigOpt))
    else
      new Properties

    // TODO: check this, different from EndToEndLatency setting
    producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0") // ensure writes are synchronous
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, options.valueOf(acksOpt))
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.EXCLUSIVE_RDMA, (!withrdmashared).toString)
  }
}

