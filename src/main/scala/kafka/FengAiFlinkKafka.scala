package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

object FengAiFlinkKafka {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    //生成配置对象
    val config = new Configuration()
    //开启flink-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    //配置webui的日志文件，否则打印日志到控制台
    config.setString("web.log.path", "/tmp/flink_log")
    //配置taskManager的日志文件，否则打印日志到控制台
    config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp/flink_log")
    //配置tm有多少个slot
    config.setString("taskmanager.numberOfTaskSlots", "12")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    //使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置为1方便调试，实际线上这行代码应该去掉
    env.setParallelism(1)
    //隔多长时间执行一次ck
    env.enableCheckpointing(1000L)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    //保存EXACTLY_ONCE
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //每次ck之间的间隔，不会重叠
    checkpointConfig.setMinPauseBetweenCheckpoints(2000L)
    //每次ck的超时时间
    checkpointConfig.setCheckpointTimeout(10L)
    //如果ck执行失败，程序是否停止
    checkpointConfig.setFailOnCheckpointingErrors(true)
    //job在执行CANCE的时候是否删除ck数据
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //指定保存ck的存储模式，因为kafka的offset比较小，
    //所以kafkaSource推荐使用MemoryStateBackend来保存offset，
    //这样速度快也不会占用过多内存
    val stateBackend = new MemoryStateBackend(10 * 1024 * 1024, false)

    env.setStateBackend(stateBackend)

    //恢复策略
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        3, // number of restart attempts
        org.apache.flink.api.common.time.Time.of(0, TimeUnit.SECONDS) // delay
      )
    )

    /* Kafka consumer */
    val kafkaConsumerProps = new Properties()
    kafkaConsumerProps.setProperty("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    kafkaConsumerProps.setProperty("group.id", "qingniu66")
    //当checkpoint启动时这里被自动设置为false
    //    kafkaConsumerProps.setProperty("enable.auto.commit","false")
    val kafkaSource = new FlinkKafkaConsumer010[KafkaEvent]("flink_event", new KafkaEventDeserializationSchema, kafkaConsumerProps)
    //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    kafkaSource.setStartFromEarliest()
    //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    //kafkaSource.setStartFromLatest()


    //指定flink的流的source为kafkaSource
    val kafkaInput: DataStream[KafkaEvent] = env.addSource(kafkaSource)

    //简单的处理逻辑主要是为了验证flink消费kafka时offset的容错性
    //这里可拓展为connect广播流，在流运算过程中达到的配置更新的目的
    //并在自定义函数中使用累加器，想想这两个需求怎么做？
    kafkaInput.assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
      .map(f => {
        //当Kafka的数据为ERROR时触发容错
        if (f.message == "ERROR") {
          println(s"${f.message},help me!!!")
          1 / 0
        }
        f
      }).print()

    env.execute("HainiuFlinkKafka")
  }
}

case class KafkaEvent(message: String, eventTime: Long)

//自定义deserializer用来反序列化kafka中的数据
class KafkaEventDeserializationSchema extends KeyedDeserializationSchema[KafkaEvent] {
  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): KafkaEvent = {
    //这里打开就可以测试kafka数据接受失败的容错
    //    println(new String(message))
    //    1/0
    KafkaEvent(new String(message), System.currentTimeMillis())
  }

  //设置了无界流
  override def isEndOfStream(nextElement: KafkaEvent): Boolean = false

  //得到自定义序列化类型
  override def getProducedType: TypeInformation[KafkaEvent] = TypeInformation.of(new TypeHint[KafkaEvent]() {})
}

//指定eventTime时的waterMark字段是那个
//并设置数据的最大超时时间
//也就是说进入流的事件时间不能比waterMark小maxOutOfOrderness
//否则就被认为是超时的数据
class CustomWatermarkExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[KafkaEvent](maxOutOfOrderness) {

  override def extractTimestamp(element: KafkaEvent): Long = {
    element.eventTime
  }
}