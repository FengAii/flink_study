package windows

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * Process版 自定义窗口 聚合函数
 * 使用的是 process方法;
 */
object ProcessWinFunOnCountWindow {
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
    val tuple = List(
      ("hainiu", "class12", "小王", 50),
      ("hainiu", "class11", "小张", 50),
      ("hainiu", "class12", "小李", 55))
    // 定义socket数据源，使用集合生成
    val input = env.fromCollection(tuple)
    //先分组，然后数据按分组进行不同的窗口，当窗口数据量达到两条时，启动process计算两条记录的平均值
    input
      .keyBy(f => f._2)
      .countWindow(2)
      .process(new AvgProcessWindowFunction)
      .print()

    env.execute()
  }

}

class AvgProcessWindowFunction extends ProcessWindowFunction[(String, String, String, Int), String, String, GlobalWindow] {

  /**
   * 分组并计算windows里所有数据的平均值
   * 跟自定义聚合函数相比, 更低级,使用process
   *
   * @param key        分组key
   * @param context    windows上下文
   * @param elements   分组的value
   * @param out        operator的输出结果
   */
  override def process(key: String, context: Context,
                       elements: Iterable[(String, String, String, Int)],
                       out: Collector[String]): Unit = {
    var sum = 0
    var count = 0
    for (in <- elements) {
      sum += in._4
      count += 1
    }

    // spark collect是拉回到本地, flink是执行的意思?
    out.collect(s"Window:${context.window} count:${count} avg:${sum/count}");
  }
}

