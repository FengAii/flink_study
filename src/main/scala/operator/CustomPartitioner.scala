package operator

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object CustomPartitioner {
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

    val tuple = List(
      ("hainiu", "class12", "小王", 50),
      ("hainiu", "class12", "小李", 55),
      ("hainiu", "class11", "小张", 50),
      ("hainiu", "class11", "小强", 45))
    // 定义数据源，使用集合生成
    val input = env.fromCollection(tuple)
    //第二个参数 _._2 是指定partitioner的key是数据中的那个字段
    input.partitionCustom(new HainiuFlinkPartitioner,_._2).print()

    env.execute()
  }
}
//由于返回的永远是1，所以所有的数据都跑到第2个分区
class HainiuFlinkPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    println(key)
    1
  }
}