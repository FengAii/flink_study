package sink

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.{ConfigConstants, Configuration, CoreOptions}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


class HainiuOutPutFormat extends OutputFormat[(String,Int)]{

  //配置outputformat
  override def configure(parameters: Configuration): Unit = {
    println("configure")
  }

  //在Sink开启的时候执行一次，比如可以在这里开启mysql的连接
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    //taskNumber第几个tak,numTasks总任务数
    println(s"taskNumber:${taskNumber},numTasks:${numTasks}")
  }

  //调用writeRecord方法，执行数据的输出
  override def writeRecord(record: (String,Int)): Unit = {
    println(record)
  }

  //在Sink关闭的时候执行一次
  //比如mysql连接用完了，给还回连接池
  override def close(): Unit = {
    println("close")
  }
}

object OutputFormatWordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    //生成配置对象
    val config = new Configuration()
    //开启spark-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    //配置webui的日志文件，否则打印日志到控制台
    config.setString("web.log.path", "/tmp/flink_log")
    //配置taskManager的日志文件，否则打印日志到控制台
    config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"/tmp/flink_log")
    //配置tm有多少个slot
    config.setString("taskmanager.numberOfTaskSlots","12")

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    env.setParallelism(1)

    val input = env.fromElements("hainiu hainiu hainiu")
    val output: DataStream[(String, Int)] = input.flatMap(f => f.split(" ")).map((_,1)).keyBy(_._1).sum(1)
    //使用自定义的outputFormat
    output.writeUsingOutputFormat(new HainiuOutPutFormat)

    env.execute()
  }
}
