package operator

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object ConnectCoFlatMap {
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

    // 获取local运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    val input1: DataStream[Long] = env.generateSequence(0,10)

    val input2: DataStream[String] = env.fromCollection(List("hainiu xueyuan dashuju"))

    //连接两个流
    val connectInput: ConnectedStreams[Long, String] = input1.connect(input2)


    //flatMap之后的泛型确定了两个流合并之后的返回类型
    val value: DataStream[String] = connectInput.flatMap[String](
      //处理第一个流的数据，需要返回String类型
      (data:Long, out:Collector[String]) => {
        out.collect(data.toString)
      },
      //处理第二个流的数据，需要返回String类型
      (data:String, out:Collector[String]) => {
        val strings: Array[String] = data.split(" ")
        for (s <- strings) {
          out.collect(s)
        }
      }
    )

    value.print()

    env.execute()

  }
}

