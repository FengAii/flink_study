package operator

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object SocketWordCount {

  def main(args: Array[String]): Unit = {

    // 生成配置对象,注意不是要引用成 hadoop
    val configuration = new Configuration()
    // 打开webui
    configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true)
    // 需要设置 日志地点, 不然没设置报错
    configuration.setString("web.log.path","f:/log/tmp/flink_log")
    configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"f:/log/tmp/flink_log")

    // 获取local运行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    // 定义 socket 源
    val text: DataStream[String] = environment.socketTextStream("localhost",6666)

    // scala 开发需要加一行隐式转换, 否则在调用 operator的时候 会报错
    import org.apache.flink.api.scala._

    // 定义 operators, 作用是解析数据, 分组, 并且求 wordcount   core api
    val wordCount: DataStream[(String, Int)] = text.flatMap(_.split(" ")).map((_,1)).keyBy(_._1).sum(1)

    // 使用 FlatMapFunction自定义函数 来完成 flatmap 和 map的组合功能;
    val value: DataStream[(String, Int)] = text.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(input: String, output: Collector[(String, Int)]): Unit = {
        val strings: Array[String] = input.split(" ")
        for (s <- strings) {
          output.collect((s, 1))
        }
      }
    }).keyBy(_._1).sum(1)

    // 定义sink 打印到控制台
    wordCount.print()
    value.print()

    // 定义任务的名称并运行
    // 注意: operator 是惰性的,只要遇到 execute才执行;
    environment.execute("SocketWordCount")

  }
}
