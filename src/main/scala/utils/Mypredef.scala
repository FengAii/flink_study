package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Mypredef {

  implicit def deleteHdfs(str : String) = new hdfsString(str)

}


class hdfsString(val outPath:String){
  def deletePath() = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val path = new Path(outPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }
}
