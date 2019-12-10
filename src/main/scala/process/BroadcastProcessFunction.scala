package process

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object BroadcastProcessFunction {

  //broadcast的类型描述，也可以在broadCastProcessFunction中重复使用
  var configStateDescriptor = new MapStateDescriptor[String, CountryConfig](
    "configBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[CountryConfig]() {}))

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

    // 定义socket数据源，使用自定义的source
    val input1 = env.socketTextStream("localhost", 6666, '\n')
    // 定义socket数据源，使用自定义的source
    val input2 = env.socketTextStream("localhost", 7777, '\n')

    //将input2变成广播流，广播到所有task中
    val broadCast: BroadcastStream[CountryConfig] = input2.map(f => {
      val strings: Array[String] = f.split(" ")
      CountryConfig(strings(0), strings(1))
    }).broadcast(configStateDescriptor)

    //生成广播连接流，这样input1的数据就被广播到所有的task中
    val value: BroadcastConnectedStream[String, CountryConfig] = input1.connect(broadCast)

    value
      .process(new HainiuBroadcastProcessFunction)
      .print()

    env.execute()

  }
}

class HainiuBroadcastProcessFunction extends BroadcastProcessFunction[String, CountryConfig, (String, String)] {

  //在所有task中都能收到广播流广播来的相同数据，并将数据保存到mapState中
  override def processBroadcastElement(value: CountryConfig, ctx: BroadcastProcessFunction[String, CountryConfig, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
    val bcs: BroadcastState[String, CountryConfig] = ctx.getBroadcastState(BroadcastProcessFunction.configStateDescriptor)
    bcs.put(value.code, value)
  }

  //处理事件流的每条记录，并从mapState中得到对应的值
  override def processElement(value: String, ctx: BroadcastProcessFunction[String, CountryConfig, (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = {
    val config: CountryConfig = ctx.getBroadcastState(BroadcastProcessFunction.configStateDescriptor).get(value)
    if (config != null) {
      out.collect((value, config.name))
    }
  }
}

case class CountryConfig(code: String, name: String)