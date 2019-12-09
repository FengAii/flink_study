package state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 * 想知道两次事件hainiu之间，一共发生多少次其他事件，分别是什么事件
 * 事件流：hainiu a a a a a f d d hainiu ad d s s d hainiu…
 * 当事件流中出现字母e时触发容错
 * 输出：
 * (8,a a a a a f d d)
 * (6,ad d s s d)
 */
object OperatorStateRecovery {
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
    //设置全局并行度为1，好让所有数据都跑到一个task中，以方便测试
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

    input
      .flatMap(new OperatorStateRecoveryRichFunction)
      .print()

    env.execute()

  }
}

//由于使用了本地状态所以需要checkpoint的snapshotState方法把本地状态放到托管状态中
class OperatorStateRecoveryRichFunction extends RichFlatMapFunction[String, (Int, String)] with CheckpointedFunction {

  //托管状态
  @transient private var checkPointCountList: ListState[String] = _

  //原始状态
  private var list: ListBuffer[String] = new ListBuffer[String]

  //flatMap函数处理逻辑
  override def flatMap(value: String, out: Collector[(Int, String)]): Unit = {
    if (value == "hainiu") {
      if (list.size > 0) {
        val outString: String = list.foldLeft("")(_ + " " + _)
        out.collect((list.size, outString))
        list.clear()
      }
    } else if (value == "e") {
      1 / 0
    } else {
      list += value
    }
  }

  //再checkpoint时存储，把正在处理的原始状态的数据保存到托管状态中
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkPointCountList.clear()
    list.foreach(f => checkPointCountList.add(f))
    println(s"snapshotState:${list}")
  }

  //从statebackend中恢复保存的托管状态，并将来数据放到程序处理的原始状态中
  // 出错一次就调用一次这里，能调用几次是根据setRestartStrategy设置的
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val lsd = new ListStateDescriptor[String]("hainiuListState", TypeInformation.of(new TypeHint[String] {}))
    checkPointCountList = context.getOperatorStateStore.getListState(lsd)
    if (context.isRestored) {
      import scala.collection.convert.wrapAll._
      for (e <- checkPointCountList.get()) {
        list += e
      }
    }
    println(s"initializeState:${list}")
  }

}