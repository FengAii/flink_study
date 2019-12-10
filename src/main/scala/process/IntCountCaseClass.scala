package process

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object IntCounterCaseClass {
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

    // 获取local运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    // 定义集合数据源，使用自定义的source
    val input1 = env.socketTextStream("localhost", 6666)

    val value3: DataStream[Parent] = input1.flatMap(new RichFlatMapFunction[String, Parent] {

      override def flatMap(value: String, out: Collector[Parent]) = {
        val strings: Array[String] = value.split(" ")
        for (s <- strings) {
          if (s.startsWith("hainiu")) {
            out.collect(MatchCount())
          } else {
            out.collect(NoMatchCount())
          }
          out.collect(CountData(s, 1))
        }
      }
    })

    val count: DataStream[CountData] = value3.filter(a =>
      a match {
        case a: CountData => true
        case _ => false
      }
    ).map(a => {
      a.asInstanceOf[CountData]
    })
    count.keyBy("word").sum(1).print()


    val count1: DataStream[Parent] = value3.filter(a =>
      a match {
        case a: MatchCount => true
        case a: NoMatchCount => true
        case _ => false
      }
    )

    //把数据拉回到client端进行操作，比如发起一次数据连接把数据统一插入
    //使用了DataStreamUtils.collect就可以省略env.execute
    import scala.collection.convert.wrapAll._
    // 想看谁,就把那个operator 拉回来, 然后取值遍历 Iterator
    val value: util.Iterator[Parent] = DataStreamUtils.collect(count1.javaStream)
    var matchT = 0
    var noMatchT = 0
    for (v <- value) {
      if (v.isInstanceOf[MatchCount]) {
        matchT += 1
      } else {
        noMatchT += 1
      }
      println(s"match:${matchT},noMatch:${noMatchT}")
    }

  }
}

class Parent()

case class MatchCount() extends Parent

case class NoMatchCount() extends Parent

case class CountData(val word: String, val num: Int) extends Parent

