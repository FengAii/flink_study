package operator

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object KeyBy {
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

    //对于元组类型来说数据的选择可以使用数字(从0开始)，keyBy(0,1)这种写法代表组合key
    //    val keyBy: KeyedStream[(String, String, String, Int), Tuple] = input.keyBy(1)

    //对于key选择来说还可以使用keySelector
    //    val keyBy: KeyedStream[(String, String, String, Int), String] = input.keyBy(new KeySelector[(String, String, String, Int),String] {
    //      override def getKey(value: (String, String, String, Int)) = {
    //        value._2
    //      }
    //    })

    //也可以用函数指定
    val keyBy: KeyedStream[(String, String, String, Int), String] = input.keyBy(_._2)

    //对于scala的元组可以使用"_1"、对于java的元组可以使用"f0"，其实也是类中属性的名字
    val max: DataStream[(String, String, String, Int)] = keyBy.maxBy("_4")
    max.print()


    val hainius = List(
      new HainiuEventKey("hainiu", "class12", "小王", 50),
      new HainiuEventKey("hainiu", "class12", "小李", 55),
      new HainiuEventKey("hainiu", "class11", "小张", 50),
      new HainiuEventKey("hainiu", "class11", "小强", 45))
    // 定义数据源，使用集合生成
    val hainiuInput = env.fromCollection(hainius)
    //对于自定义类型来说也可以用类中的字段名称，记住这个自定义类型必须得是样例类
    val hainiuKeyBy: KeyedStream[HainiuEventKey, Tuple] = hainiuInput.keyBy("b")
    hainiuKeyBy.maxBy("d").print()
    hainiuKeyBy.map(f => new HainiuEventValue(f.a,f.b,f.c,f.d)).print("a")

    env.execute("key by")

  }
}
//样例类，可以用于key，因为其默认实现了hashCode方法，可用于对象比较，当然也可用于value
case class HainiuEventKey(a:String,b:String,c:String,d:Int)
//普通类可以用于非key，只能用于value
class HainiuEventValue(a:String,b:String,c:String,d:Int)