package operator

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * TODO 尚未解决 默认的并行度的问题, 使用多少个slot
 */

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
    println("======"+input.parallelism)
    input.partitionCustom(new HainiuFlinkPartitioner,_._2).print()
    // 并行度自动计算的, 怎么计算的不太明确, .setParallelism(3) 可以设置并行度;
    println(env.getExecutionPlan)
    env.execute()


  }
}

//由于返回的永远是1，所以所有的数据都跑到第2个分区
//常数1 决定了 数据都跑到那个分区
class HainiuFlinkPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    // numPartitions 是下游算子的并发数
    // 所以传回的数字 ,只能是 0~numPartitions-1
    // key.hashcode % numPartitions
    //---未确定 通过观察图发现 这里 sink 这里一个print , 后面一个print 并行度为8
    // 上面加了 println之后 就是并行度8了; 但是可以设置并行度;
    println(key)
    println(numPartitions)
    1
  }
}