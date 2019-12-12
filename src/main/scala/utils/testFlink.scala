package utils

class testFlink extends baseFlink {
  override def setFlinkWebUI: Boolean = ???

  override def setFlinkWebLogPath: String = ???

  override def setFlinkTmLogPath: String = ???

  override def setFlinkTmNum: Int = ???

  override def setIsLocal: Boolean = false





}

object testFlink{


  def main(args: Array[String]): Unit = {

    val flink = new testFlink


  }
}