package sink

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

class HainiuSink extends SinkFunction[(String,Int)] {
  override def invoke(value: (String,Int), context: SinkFunction.Context[_]): Unit = {
    println(s"value:${value}," +
      s"processTime:${context.currentProcessingTime()}," +
      s"waterMark:${context.currentWatermark()}")
  }
}

object SinkFunctionWordCount {
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

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    env.setParallelism(1)

    val input = env.fromElements("hainiu hainiu hainiu")
    val output: DataStream[(String, Int)] = input.flatMap(f => f.split(" ")).map((_, 1)).keyBy(_._1).sum(1)

    // 使用自定义的sink
    output.addSink(new HainiuSink)

    env.execute()
  }
}