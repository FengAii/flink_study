package windows

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 自定义 聚合函数
 */
object AggFunctionOnCountWindow {
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
    //先分组，然后数据按分组进行不同的窗口，当窗口数据量达到两条时，启动aggregate计算两条记录的分组合
    input
      .keyBy(1)
      .countWindow(2)
      .aggregate(new SumAggregate)
      .print()

    env.execute()

  }
}


/**
 * 自定义的聚合函数
 * 1. 需要一个初始状态的累加器
 * 2. add方法, 就是数据来了之后怎么进行计算
 * 3. getResult 方法, 就是调用 最后的结果
 * 4. merge就是 不同的partitioner 进行合并的时候, 比如两个累加器怎么聚合;
 */
class SumAggregate extends AggregateFunction[(String, String, String, Int), (String, Long), (String, Long)] {
  /**
   * 创建累加器来保存中间状态(name和count)
   */
  override def createAccumulator(): (String, Long) = {
    ("", 0L)
  }

  /**
   * 将元素添加到累加器并返回新的累加器value
   */
  override def add(value: (String, String, String, Int), accumulator: (String, Long)): (String, Long) = {
    (s"${value._3}\t${accumulator._1}", accumulator._2 + value._4)
  }

  /**
   * 从累加器提取结果
   */
  override def getResult(accumulator: (String, Long)): (String, Long) = {
    accumulator
  }

  /**
   * 合并两个累加器并返回
   */
  override def merge(a: (String, Long), b: (String, Long)): (String, Long) = {
    (s"${a._1}\t${b._1}", a._2 + b._2)
  }
}
