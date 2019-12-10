package process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.util.Random

object ProcessFunction {
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
    val input: DataStream[UserTime] = env.addSource(new RandomSourceFunction)
      //指定数据水位线标识
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserTime]() {
        override def extractAscendingTimestamp(element: UserTime) = element.time
      })
    //打印原始输入数据
    input.print()
    //打印3秒钟没有更新，没有进行更新的key的统计值
    input.keyBy("user")
      .process(new CountWithTimeoutFunction)
      .print()

    env.execute()

  }
}

//每隔一秒随机发送一条数据
class RandomSourceFunction extends SourceFunction[UserTime] {

  @volatile private var isRunning = true

  private val arr = Array("小王", "小强", "小张", "小猫", "小狗")

  override def run(ctx: SourceFunction.SourceContext[UserTime]): Unit = {
    while (isRunning) {
      val i: Int = Random.nextInt(arr.length)
      val ut = UserTime(arr(i), System.currentTimeMillis())
      ctx.collect(ut)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = isRunning = false
}

//按key计数，如果某个key在3秒之内没有新的数据到来就发出(key,count)
class CountWithTimeoutFunction extends ProcessFunction[UserTime, (String, Long)] {

  //使用自定义类型的keyedState，用于存储每个key的数据的处理结果，并在timeService中使用
  private var state: ValueState[CountWithTime] = null

  //恢复state，由于没有设置checkpoint存储和恢复策略所以无法使用该state进行容错，仅仅用于存储
  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTime]("hainiuCountState", classOf[CountWithTime]))
  }

  //timeService的时间间隔
  private val intervalTime = 3000

  //对每个key的数据的count值进行累加，并将累加结果放到keyedState中
  override def processElement(value: UserTime, ctx: ProcessFunction[UserTime, (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    var current: CountWithTime = state.value()
    if (current == null) {
      current = CountWithTime()
      current.key = value.user
    }

    current.count += 1

    current.lastTime = ctx.timestamp()

    //更新keyedState
    state.update(current)

    //对每个key注册timeService服务，注意每个key在相同的时间只能注意一个timeService
    //timeService就是启动一个定时器，在指定时间到达之后调用onTimer方法
    ctx.timerService().registerEventTimeTimer(current.lastTime + intervalTime)
  }

  //在这个key的timeService达到之后被执行
  override def onTimer(timestamp: Long, ctx: ProcessFunction[UserTime, (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    //如果key的lastTime没有被更新，也就是说这个key没有新的数据到来
    //那么在指定时间的timeService的时候，lastTime将与其注册的timeService时的时间相等
    //在时间相等时输出该key的统计值
    val result: CountWithTime = state.value()
    if(timestamp == result.lastTime + intervalTime){
      out.collect((result.key,result.count))
    }

  }
}
//自定义keyedState的存储类型
case class CountWithTime(var key: String = "", var count: Int = 0, var lastTime: Long = 0)
//自定义源产生的数据类型
case class UserTime(user: String, time: Long)
