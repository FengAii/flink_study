package operator

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

object SplitAndSelect {
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

    val input: DataStream[Long] = env.generateSequence(0, 10)

    val splitStream: SplitStream[Long] = input.split(f => {
      val out = new ListBuffer[String]
      //返回数据的拆分标记
      if (f % 2 == 0) {
        out += "hainiu"
      } else {
        out += "dashuju"
      }
      out
    })
    //根据拆分标记选择数据
    //    splitStream.select("hainiu").print()
    //    splitStream.select("dashuju").print()
    splitStream.select("hainiu","dashuju").print()

    env.execute()
  }
}
