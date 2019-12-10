package process

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.util.Random

object CoProcessFunction {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    //生成配置对象
    val config = new Configuration()
    //开启spark-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    //配置webui的日志文件，否则打印日志到控制台
    config.setString("web.log.path", "/tmp/flink_log")
    //配置taskManager的日志文件，否则打印日志到控制台
    config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp/flink_log")
    //配置tm有多少个slot
    config.setString("taskmanager.numberOfTaskSlots", "12")
    // 获取local运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    //使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //添加自定义随机源
    val input1: DataStream[CoUserTime] = env.addSource(new CoRandomSourceFunction("input1"))
      //指定数据水位线标识
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[CoUserTime]() {
        override def extractAscendingTimestamp(element: CoUserTime) = element.time
      })

    //添加自定义随机源
    val input2: DataStream[CoUserTime] = env.addSource(new CoRandomSourceFunction("input2"))
      //指定数据水位线标识
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[CoUserTime]() {
        override def extractAscendingTimestamp(element: CoUserTime) = element.time
      })

    val conn: ConnectedStreams[CoUserTime, CoUserTime] = input1.connect(input2)

    conn.process(new HainiuCoProcessFunction)
      .print()

    env.execute()

  }
}
//每隔2秒随机发送一条数据
class CoRandomSourceFunction(inputTag:String) extends SourceFunction[CoUserTime] {

  @volatile private var isRunning = true

  private val arr = Array("小王", "小强", "小张", "小猫", "小狗")

  override def run(ctx: SourceFunction.SourceContext[CoUserTime]): Unit = {
    while (isRunning) {
      val i: Int = Random.nextInt(arr.length)
      val ut = CoUserTime(s"${inputTag}:${arr(i)}", System.currentTimeMillis())
      ctx.collect(ut)
      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = isRunning = false
}


class HainiuCoProcessFunction extends CoProcessFunction[CoUserTime,CoUserTime,String] {
  override def processElement1(value: CoUserTime, ctx: CoProcessFunction[CoUserTime, CoUserTime, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"${value}")
  }

  override def processElement2(value: CoUserTime, ctx: CoProcessFunction[CoUserTime, CoUserTime, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"${value}")
  }
}

case class CoUserTime(user: String, time: Long)