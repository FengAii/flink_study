package state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import scala.collection.mutable

/**
 * 将输入格式为"字符串 数字"的字符串转换成(字符串,数字)的元组类型
 * 事件流：hainiu 666
 * 当事件流中出现"任意字符串 888"时触发容错
 * 输出：
 * (hainiu,666)
 */
object KeyedStateRecovery {
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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    //并行度设置为1，是想让所有的key都跑到一个task中，以方便测试
    env.setParallelism(1)

    //隔多长时间执行一次ck
    env.enableCheckpointing(1000L)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    //保存EXACTLY_ONCE
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //每次ck之间的间隔，不会重叠
    checkpointConfig.setMinPauseBetweenCheckpoints(2000L)
    //每次ck的超时时间
    checkpointConfig.setCheckpointTimeout(10L)
    //如果ck执行失败，程序是否停止
    checkpointConfig.setFailOnCheckpointingErrors(true)
    //job在执行CANCE的时候是否删除ck数据
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //指定保存ck的存储模式
    val stateBackend = new FsStateBackend("file:///Users/leohe/Data/output/flink/checkpoints", true)

    //        val stateBackend = new MemoryStateBackend(10 * 1024 * 1024,false)

    //    val stateBackend = new RocksDBStateBackend("hdfs://ns1/flink/checkpoints",true)

    env.setStateBackend(stateBackend)

    //恢复策略
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        3, // number of restart attempts
        Time.of(0, TimeUnit.SECONDS) // delay
      )
    )

    val input: DataStream[String] = env.socketTextStream("localhost", 6666)

    //因为KeyedStateRichFunctionString中使用了keyState，所以它必须在keyBy算子的后面
    input
      .map(f => {
        val strings: mutable.ArrayOps[String] = f.split(" ")
        (strings(0), strings(1).toInt)
      })
      .keyBy(0)
      .flatMap(new KeyedStateRecoveryRichFunctionString)
      .print()

    env.execute()
  }
}

//由于没有使用本地的状态所以不需要实现checkpoint接口
class KeyedStateRecoveryRichFunctionString extends RichFlatMapFunction[(String, Int), (String, Int)] {

  //ValueState是Key的state类型，是只能存在于KeyedStream的operator中
  @transient private var sum: ValueState[(String, Int)] = null

  override def flatMap(value: (String, Int), out: Collector[(String, Int)]): Unit = {

    println(s"state value:${sum.value()}")

    //当value值为888时，触发异常
    if (value._2 != 888) {
      sum.clear()
      sum.update(value)
      out.collect(value)
    } else {
      1 / 0
    }
  }

  //在operator启动时执行一次
  //如果operator出现异常，在恢复operator时会被再次执行
  override def open(parameters: Configuration): Unit = {

    //keyState的TTL策略
    val ttlConfig = StateTtlConfig
      //keyState的超时时间为100秒
      .newBuilder(Time.seconds(100))
      //当创建和更新时，重新计时超时时间
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      //失败时不返回keyState的值
      //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      //失败时返回keyState的值
      .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
      //ttl的时间处理等级目前只支持ProcessingTime
      .setTimeCharacteristic(StateTtlConfig.TimeCharacteristic.ProcessingTime)
      .build

    //从runtimeContext中获得ck时保存的状态
    val descriptor = new ValueStateDescriptor[(String, Int)]("hainiuValueState", TypeInformation.of(new TypeHint[(String, Int)] {}))
    descriptor.enableTimeToLive(ttlConfig)
    sum = getRuntimeContext.getState(descriptor)
  }
}