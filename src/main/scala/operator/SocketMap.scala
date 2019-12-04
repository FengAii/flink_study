package operator

import java.util


import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object SocketMap {
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

    // 定义socket数据源，使用自定义的source
    val text = env.socketTextStream("localhost", 6666, '\n')

    //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
    import org.apache.flink.api.scala._

    // 定义operators，作用是解析数据, 分组
    val windowCounts:DataStream[(String,Int)] = text
      .flatMap(s => s.split(" "))
      .map(s => (s,1))

    // 定义sink打印输出
    windowCounts.print()

    //把数据拉回到client端进行操作，比如发起一次数据连接把数据统一插入
    //使用了DataStreamUtils.collect就可以省略env.execute
    import scala.collection.convert.wrapAll._
    val value: util.Iterator[(String, Int)] = DataStreamUtils.collect(windowCounts.javaStream)
    for(v <- value){
      println(v)
    }

    //定义任务的名称并运行
    //注意：operator是惰性的，只有遇到env.execute才执行
    env.execute("Socket WordCount")
  }
}