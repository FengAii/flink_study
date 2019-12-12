package utils

import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


abstract class baseFlink {

  var env: StreamExecutionEnvironment = null

  def setFlinkWebUI: Boolean

  def setFlinkWebLogPath: String

  def setFlinkTmLogPath: String

  def setFlinkTmNum: Int

  def setIsLocal: Boolean

  def setConf(configuration : Configuration ): Configuration = {
    //开启flink-webui
    configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, setFlinkWebUI)
    //配置webui的日志文件，否则打印日志到控制台
    configuration.setString("web.log.path", setFlinkWebLogPath)
    //配置taskManager的日志文件，否则打印日志到控制台
    configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, setFlinkTmLogPath)
    //配置tm有多少个slot
    configuration.setString("taskmanager.numberOfTaskSlots", String.valueOf(setFlinkTmNum))

    return configuration
  }

  def getExecution( is_local : Boolean ): Unit = {

    if (is_local)
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(setConf(new Configuration()))
    else
      env = StreamExecutionEnvironment.getExecutionEnvironment

  }

  def run () ={}

//TODO
  /**
   * 剩下的就思路清晰了， 这里这个 baseFlink 就是为了生成 配置，使用
   * 什么源头， 什么配置
   * 然后自己写一个类 继承， 就可以少些东西
   * 然后 在自己的 object 直接写 业务逻辑 就可以了 ok， 清楚，明白
   */

}
