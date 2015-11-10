package moe.brianhsu.fmmaster

object HelloWorld {
  def main(args: Array[String]) {
    val t = new FMMasterDirectory("/mnt/WinD/Test")
    t.updateFileIndex()
    t.startWatching()
    println("DONE....")
  }
}
