package myFirstFrame

import java.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}


/**
 * 使用 flink 来处理 之前 spark 处理的问题;
 *
 * groupby 搭配 reduce  可以尝试使用 reduceGroup来进行操作,可能会简单一些;
 */

object Line2File_usa {

  def main(args: Array[String]): Unit = {

    import utils.Mypredef.deleteHdfs

    val input_Path = "F:\\tmp\\mihoyo\\input\\asia"
    val output_Path = "F:\\tmp\\flink\\output\\2"

    output_Path.deletePath()

    // 获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = env.readTextFile(input_Path)

    import org.apache.flink.api.scala._


    val value: DataSet[String] = text
      //  每一行切割
      .map(_.split("\t"))
      //  把每一行 重构, 不需要使用 flatmap
      .map(f => (f(1), f(0)))
      //  并行度为1 ,不然 文件不为一个(可能有其他方法), 排序的时候 可以全都排
      .setParallelism(1)
      //  排序,貌似 flink 只有局部排序
      .sortPartition(1, Order.ASCENDING)
      //  groupBy 不返回新的 DataSet
      .groupBy(0)
      //  reduce 聚合,写函数
      .reduce { (w1, w2) => (w1._1, w1._2 + "\t" + w2._2) }
      //  让每个 key 对应的 value 也按照顺序排序
      .map(f => {
        val strings: Array[String] = f._2.split("\t")
        val ints: Array[Int] = strings.map(f => Integer.parseInt(f))
        util.Arrays.sort(ints)
        val sb: StringBuffer = new StringBuffer()
        for (i <- 0 until (ints.length))
          sb.append(ints(i)).append("\t")
        sb.deleteCharAt(sb.length() - 1)
        (f._1, sb.toString)
      })
      //  并行度为1
      .setParallelism(1)
      //  排序,让key 从小到大,这是根据数据特点做的
      .sortPartition(f => f._2.split("\t")(0), Order.ASCENDING)
      //  去除 元组的 括号
      .map(f => f._1 + "\t" + f._2)


    value.writeAsText(output_Path)

    env.execute()

  }

}

