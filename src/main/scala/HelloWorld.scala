package moe.brianhsu.fmmaster

import akka.actor._
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.SimpleFileVisitor
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.FileVisitResult
import moe.brianhsu.fmmaster.util._

case class BasicFileInfo(filePath: String, size: Long, lastModifiedTime: Long)

class FMMasterDirActor(dirLocation: String) extends Actor {
  
  val dirPath = Paths.get(dirLocation)

  override def preStart() {
    class FileLister extends SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
        val relativePath = dirPath.relativize(file)
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(dirPath, new FileLister)
  }

  def receive = {
    case s: String => println(s"I got $s and dirLocation: $dirLocation")
  }

}

object HelloWorld {
  def main(args: Array[String]) {

    val actorSystem = ActorSystem("FMMaster")
    val fmMasterDirActor = actorSystem.actorOf(Props(classOf[FMMasterDirActor], "/mnt/WinD/MEGA"))

    println("Hello World")
  }
}
