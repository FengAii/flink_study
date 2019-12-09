package windows

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ReduceFunctionOnCountWindow {
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
    val tuple = List(
      ("hainiu", "class12", "小王", 50),
      ("hainiu", "class12", "小李", 55),
      ("hainiu", "class11", "小张", 50))
    // 定义socket数据源，使用集合生成
    val input = env.fromCollection(tuple)

    //先分组，然后数据按分组进行不同的窗口，当窗口数据量达到两条时，启动reduce计算两条记录的分组合
    val windows: DataStream[(String, String, String, Int)] = input.keyBy(1).countWindow(2).reduce((a,b) =>(a._1,a._2,a._3+b._3,a._4 + b._4))
    windows.print()

    env.execute()
  }
}