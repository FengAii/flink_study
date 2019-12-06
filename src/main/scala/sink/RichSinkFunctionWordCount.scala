package sink

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


class HainiuRichSink extends RichSinkFunction[(String, Int)] {

  //在Sink开启的时候执行一次，比如可以在这里开启mysql的连接
  override def open(parameters: Configuration): Unit = {
    println("open")
  }

  //在Sink关闭的时候执行一次
  //比如mysql连接用完了，给还回连接池
  override def close(): Unit = {
    println("close")
  }

  //调用invoke方法，执行数据的输出
  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //在rich方法中可以使用getRuntimeContext方法得到比如广播变量和累加器
    //getRuntimeContext.getBroadcastVariable("")
    println(s"value:${value}," +
      s"processTime:${context.currentProcessingTime()}," +
      s"waterMark:${context.currentWatermark()}")
  }
}

object RichSinkFunctionWordCount {
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
    output.addSink(new HainiuRichSink)

    env.execute()
  }
}