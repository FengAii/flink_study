package process

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object KeyedBroadcastProcessFunction {

  //broadcast的类型描述，也可以在broadCastProcessFunction中重复使用
  var configStateDescriptor = new MapStateDescriptor[String, CountryConfigKeyed]("configBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[CountryConfigKeyed]() {}))

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
    val broadCast: BroadcastStream[CountryConfigKeyed] = input2.map(f => {
      val strings: Array[String] = f.split(" ")
      CountryConfigKeyed(strings(0), strings(1))
    }).broadcast(configStateDescriptor)


    //生成广播连接流，这样input1的数据就被广播到所有的task中
    input1.map((_, 1))
      .keyBy(_._1)
      .connect(broadCast)
      .process(new HainiuKeyedBroadcastProcessFunction())
      .print()

    env.execute()
  }
}

//KeyedBroadcastProcessFunction与BroadcastProcessFunction不同的是
//在processElement方法中可以使用ctx.getCurrentKey能拿到input1的key
//并且connect的input1必须是KeyedStream
class HainiuKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, (String, Int), CountryConfigKeyed, (String, String)] {

  //处理事件流的每条记录，并从mapState中得到对应的值
  override def processElement(value: (String, Int), ctx: KeyedBroadcastProcessFunction[String, (String, Int), CountryConfigKeyed, (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = {
    val config: CountryConfigKeyed = ctx.getBroadcastState(KeyedBroadcastProcessFunction.configStateDescriptor).get(value._1)
    if (config != null) {
      out.collect((value._1, s"${config.name},key:${ctx.getCurrentKey}"))
    }
  }

  //在所有task中都能收到广播流广播来的相同数据，并将数据保存到mapState中
  override def processBroadcastElement(value: CountryConfigKeyed, ctx: KeyedBroadcastProcessFunction[String, (String, Int), CountryConfigKeyed, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
    val bcs: BroadcastState[String, CountryConfigKeyed] = ctx.getBroadcastState(KeyedBroadcastProcessFunction.configStateDescriptor)
    bcs.put(value.code, value)
  }
}

case class CountryConfigKeyed(code: String, name: String)