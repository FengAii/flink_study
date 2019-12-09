package windows

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class HainiuClass(school:String,classs:String,name:String,time:Long)
case class HainiuResult(name:String,result:Int,time:Long)
case class HainiuJoin(classs:String,name:String,result:Int)
object IntervalJoin {
  def main(args: Array[String]): Unit = {

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

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    //设置流数据处理的时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tuple = List(
      HainiuClass("hainiu", "class12", "小王", System.currentTimeMillis),
      HainiuClass("hainiu", "class12", "小李", System.currentTimeMillis),
      HainiuClass("hainiu", "class11", "小张", System.currentTimeMillis),
      HainiuClass("hainiu", "class11", "小强", System.currentTimeMillis))

    val tuple2 = List(
      HainiuResult("小王", 88, System.currentTimeMillis),
      HainiuResult("小李", 88, System.currentTimeMillis),
      HainiuResult("小张", 88, System.currentTimeMillis),
      HainiuResult("小强", 88, System.currentTimeMillis))

    val input1:DataStream[HainiuClass] = env.fromCollection(tuple).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[HainiuClass]() {
      override def extractAscendingTimestamp(element: HainiuClass) = {
        element.time
      }
    })

    val input2: DataStream[HainiuResult] = env.fromCollection(tuple2).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[HainiuResult]() {
      override def extractAscendingTimestamp(element: HainiuResult) = {
        element.time
      }
    })

    val keyedStream: KeyedStream[HainiuClass, String] = input1.keyBy(_.name)

    val otherKeyedStream: KeyedStream[HainiuResult, String] = input2.keyBy(_.name)

    //e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound
    // key1 == key2 && leftTs - 20 <= rightTs <= leftTs + 20
    keyedStream.intervalJoin(otherKeyedStream)
      .between(Time.milliseconds(-20), Time.milliseconds(20))
      .upperBoundExclusive()
      .lowerBoundExclusive()
      .process(new ProcessJoinFunction[HainiuClass, HainiuResult, HainiuJoin]() {
        override def processElement(left: HainiuClass,
                                    right: HainiuResult,
                                    ctx: ProcessJoinFunction[HainiuClass, HainiuResult, HainiuJoin]#Context,
                                    out: Collector[HainiuJoin]) = {
          out.collect(HainiuJoin(left.classs,left.name,right.result))
        }
      }).print()

    env.execute("IntervalJoin")
  }
}
