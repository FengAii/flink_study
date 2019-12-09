package windows

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

/**
 * 而 join 侧重的是pair，是对同一个key上的每对元素进行操作
 * 类似inner join
 * 按照一定条件分别取出两个流中匹配的元素，返回给下游处理
 * Join是cogroup 的特例
 * 只能在window中用
 */
object JoinOnSessionWindow {
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

    // 定义socket数据源1
    val input1 = env.socketTextStream("localhost", 6666, '\n')
    val map1: DataStream[(String, Int)] = input1.flatMap(_.split(" ")).map((_,1))

    // 定义socket数据源2
    val input2 = env.socketTextStream("localhost", 8888, '\n')
    val map2: DataStream[(String, Int)] = input2.flatMap(_.split(" ")).map((_,1))


    /**
     * 1、创建两个个socket stream。输入的字符串以空格为界分割成Array[String]。然后再取出其中前两个元素组成(String, String)类型的tuple。
     * 2、join条件为两个流中的数据((String, String)类型)第一个元素相同。
     * 3、为测试方便，这里使用session window。只有两个元素到来时间前后相差不大于10秒之时才会被匹配。
     * Session window的特点为，没有固定的开始和结束时间，只要两个元素之间的时间间隔不大于设定值，就会分配到同一个window中，否则后来的元素会进入新的window。
     * 4、将window默认的trigger修改为count trigger。这里的含义为每到来一个元素，都会立刻触发计算。
     * 5、处理匹配到的两个数据，例如到来的数据为(1, "hainiu")和(1, "hainiu")，输出到下游则为"hainiu == hainiu"
     * 6、结论：
     *  a、join只返回匹配到的数据对。若在window中没有能够与之匹配的数据，则不会有输出。
     *  b、join会输出window中所有的匹配数据对。
     *  c、不在window内的数据不会被匹配到。
     * */
    map1.join(map2)
      .where(_._1)
      .equalTo(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .trigger(CountTrigger.of(1))
      .apply((a,b) => {
        s"${a._1} == ${b._1}"
      }).print()

    env.execute()
  }
}
