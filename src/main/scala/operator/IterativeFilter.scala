package operator

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

//输入一组数据，我们对他们分别进行减1运算，直到等于0为止
object IterativeFilter {
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

    //设置全局并行度为1
    env.setParallelism(1)
    // 生成包含一个0到10的DStream
    val input = env.generateSequence(0, 10)

    //流中的元素每个减1，并过滤出大于0的，然后生成新的流
    val value: DataStream[Long] = input.iterate(
      d => (d.map(_ - 1),
        d.filter(_ > 0)))
    value.print()

    env.execute("IterativeFilter")
  }

}