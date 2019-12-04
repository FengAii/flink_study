package source

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


//RichParallelSourceFunction不但能并行化
//还比ParallelSourceFunction增加了open和close方法、getRuntimeContext
class HainiuRichParallelSource extends RichParallelSourceFunction[String] {
  var num = 0
  var isCancel = true

  //在source开启的时候执行一次，比如可以在这里开启mysql的连接
  override def open(parameters: Configuration): Unit = {
    println("open")
    num = 1000
  }

  //在source关闭的时候(run运行完成或者用户主动点webUI上的canceling)执行一次
  //比如mysql连接用完了，给还回连接池
  override def close(): Unit = {

    while (isCloseMysql){
      Thread.sleep(1000)
    }

    println("close")
    num = 0
  }

  //调用run方法向下游产生数据
  //手动cancel之后，不会等待run方法中处理结束而是强制执行close方法
  //这样就可能导致run方法中正在使用的连接被close了
  //所以此时需要加一个处理完成标识，用于判断是否可以进行close
  var isCloseMysql = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    import scala.util.control.Breaks._
    breakable {
      while (isCancel) {
        println(getRuntimeContext.getIndexOfThisSubtask)

        ctx.collect(s"hainiu_${num}")
        Thread.sleep(2000)
        num += 1

        //source的退出条件
        if (num >= 1005) break()


      }
    }
    isCloseMysql = false
  }

  //在输出的时候被执行，传递变量用于控制run方法中的执行
  //这个是被手动触发，在执行完cancel之后，会再执行close
  override def cancel(): Unit = {
    println("caceling")
    isCancel = false
  }
}

object RichParallelSourceFunctionWordCount {
  def main(args: Array[String]): Unit = {
    //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
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

    // 使用自定义的source
    val text = env.addSource(new HainiuRichParallelSource)

    // 定义operators，作用是解析数据, 分组, 窗口化, 并且聚合求SUM
    val windowCounts = text.setParallelism(2).flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]) = {
        val strings: Array[String] = value.split(" ")
        for (s <- strings) {
          out.collect((s, 1))
        }
      }
    }).setParallelism(2).keyBy(0)
      .sum(1).setParallelism(2)

    // 定义sink打印输出
    windowCounts.slotSharingGroup("hainiu").print().setParallelism(2)

    //打印任务执行计划
    println(env.getExecutionPlan)

    //定义任务的名称并运行
    //注意：operator是惰性的，只有遇到env.execute才执行
    env.execute("Socket Window WordCount")
  }
}