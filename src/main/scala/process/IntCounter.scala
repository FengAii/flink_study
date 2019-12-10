package process

import java.util

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object IntCounter {
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

    // 定义集合数据源，使用自定义的source
    val input1 = env.fromElements("a a a","b b b")

    input1.flatMap(new RichFlatMapFunction[String,(String,Int)] {

      var inc:IntCounter = null

      override def open(parameters: Configuration) = {
        inc = new IntCounter
        getRuntimeContext.addAccumulator("hainiu", this.inc);
      }

      override def flatMap(value: String, out: Collector[(String, Int)]) = {
        val strings: Array[String] = value.split(" ")
        for(s <- strings){
          this.inc.add(1)
          out.collect((s,1))
        }
      }
    }).keyBy(0)
      .sum(1)
      .print()

    val res: JobExecutionResult = env.execute("aaaa")
    //这个是程序运行之后得到的累加器，他是多个subTask里的累加器聚合的结果
    val num: Int = res.getAccumulatorResult[Int]("hainiu")
    println(num)


    //把数据拉回到client端进行操作，比如发起一次数据连接把数据统一插入
    //使用了DataStreamUtils.collect就可以省略env.execute
    // cli 拉取数据, 完成 自带办不到的地方, 自带的得等停
//    import scala.collection.convert.wrapAll._
//    val value: util.Iterator[(String, Int)] = DataStreamUtils.collect(windowCounts.javaStream)
//    for(v <- value){
//      println(v)
//    }


  }
}
