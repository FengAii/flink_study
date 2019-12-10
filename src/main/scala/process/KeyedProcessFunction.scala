package process

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.util.Random

object KeyedProcessFunction {
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
    val input: DataStream[KeyedUserTime] = env.addSource(new KeyedRandomSourceFunction)
      //指定数据水位线标识
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[KeyedUserTime]() {
        override def extractAscendingTimestamp(element: KeyedUserTime) = element.time
      })
    //打印原始输入数据
    input.print()

    input.keyBy(_.user)
      .process(new KeyedCountWithTimeoutFunction)
      .print()

    env.execute()
  }
}

//每隔一秒随机发送一条数据
class KeyedRandomSourceFunction extends SourceFunction[KeyedUserTime] {

  @volatile private var isRunning = true

  private val arr = Array("小王", "小强", "小张", "小猫", "小狗")

  override def run(ctx: SourceFunction.SourceContext[KeyedUserTime]): Unit = {
    while (isRunning) {
      val i: Int = Random.nextInt(arr.length)
      val ut = KeyedUserTime(arr(i), System.currentTimeMillis())
      ctx.collect(ut)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = isRunning = false
}

//KeyedProcessFunction与ProcessFunction的不同之后就是可以获得key的值
class KeyedCountWithTimeoutFunction extends KeyedProcessFunction[String, KeyedUserTime, (String, Long)] {
  //timeService的时间间隔
  private val intervalTime = 3000

  override def processElement(value: KeyedUserTime, ctx: KeyedProcessFunction[String, KeyedUserTime, (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    println(s"getKey:${ctx.getCurrentKey}")
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + intervalTime)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, KeyedUserTime, (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    out.collect((ctx.getCurrentKey,ctx.timestamp()))
  }
}

//自定义源产生的数据类型
case class KeyedUserTime(user: String, time: Long)
