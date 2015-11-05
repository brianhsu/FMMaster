package moe.brianhsu.fmmaster

import akka.actor._
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.SimpleFileVisitor
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.FileVisitResult
import moe.brianhsu.fmmaster.util._
import java.io.File

case class SHA1FileInfo(filePath: String, size: Long, lastModifiedTime: Long, sha1: String) {
  def file(implicit dirPath: Path) = new File(dirPath + File.separator + filePath)
}
case class BasicFileInfo(filePath: String, size: Long, lastModifiedTime: Long) {
  def file(implicit dirPath: Path) = new File(dirPath + File.separator + filePath)
}

class FMMasterDirActor(dirLocation: String) {
  
  import moe.brianhsu.fmmaster.model.DatabaseDSL
  import moe.brianhsu.fmmaster.model.DatabaseDSL._
  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val dirPath = Paths.get(dirLocation)

  def getShouldUpdate(basicFileInfoList: List[BasicFileInfo]) =  {
    using(dirPath) {
      basicFileInfoList.filter { basicFileInfo =>
        val file = FMMasterDirDB.fileIndex.where(_.filePath === basicFileInfo.filePath).headOption
        val filteredFile = file.filter(f => f.size == basicFileInfo.size && f.lastModifiedTime == basicFileInfo.lastModifiedTime).toList
        filteredFile.isEmpty
      }
    }
  }

  def getFileSHA1Checksum(basicFileInfo: BasicFileInfo) = Future {
    SHA1Utils.sha1Checksum(basicFileInfo.file(dirPath))
  }

  def updateFileIndex(basicFileInfo: BasicFileInfo, sha1: String) = Future {
    if (basicFileInfo.file.exists) {
      using(dirPath) {
        val fileHolder = FMMasterDirDB.fileIndex.where(_.filePath === basicFileInfo.filePath).headOption
        fileHolder match {
          case None =>
            FMMasterDirDB.fileIndex.insert(
              new FileIndex(sha1, basicFileInfo.filePath, basicFileInfo.size, basicFileInfo.lastModifiedTime)
            )
          case Some(file) =>
            file.sha1 = sha1
            file.size = basicFileInfo.size
            file.lastModifiedTime = basicFileInfo.lastModifiedTime
            FMMasterDirDB.fileIndex.update(file)
        }
      }
    }
  }

  def updateFileIndex(fileList: List[SHA1FileInfo]) = Future {
    using(dirPath) {
      fileList.foreach { sha1FileInfo =>
        if (sha1FileInfo.file.exists) {
          val fileHolder = FMMasterDirDB.fileIndex.where(_.filePath === sha1FileInfo.filePath).headOption
          fileHolder match {
            case None =>
              FMMasterDirDB.fileIndex.insert(
                new FileIndex(sha1FileInfo.sha1, sha1FileInfo.filePath, sha1FileInfo.size, sha1FileInfo.lastModifiedTime)
              )
            case Some(file) =>
              file.sha1 = sha1FileInfo.sha1
              file.size = sha1FileInfo.size
              file.lastModifiedTime = sha1FileInfo.lastModifiedTime
              FMMasterDirDB.fileIndex.update(file)
          }
        }
      }
    }
  }

  def getSHA1FileInfo(basicFileInfo: BasicFileInfo): Future[Option[SHA1FileInfo]] = {
    val futureResult = for {
      sha1Checksum <- getFileSHA1Checksum(basicFileInfo)
    } yield {
      Some(SHA1FileInfo(basicFileInfo.filePath, basicFileInfo.size, basicFileInfo.lastModifiedTime, sha1Checksum))
    } 
    
    futureResult.recover {
      case e: Exception => None
    }
  }

  def updateFileIndex() {
    var files: List[BasicFileInfo] = Nil
    val startTime = System.currentTimeMillis
    class FileLister extends SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
        val relativePath = dirPath.relativize(file)
        val basicFileInfo = BasicFileInfo(relativePath.normalize.toString, attr.size, attr.lastModifiedTime.toMillis)
        files ::= basicFileInfo
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(dirPath, new FileLister)

    import scala.concurrent.Await
    import scala.concurrent.duration._

    println("Got list:" + ((System.currentTimeMillis - startTime) / 1000.0))

    println("Wait for response...., size:" + files.size)

    val shouldUpdateList: List[BasicFileInfo] = getShouldUpdate(files)

    println("Filtred result:" + ((System.currentTimeMillis - startTime) / 1000.0))
    val sha1FutureList = shouldUpdateList.map { getSHA1FileInfo }
    val xs2: Future[List[Option[SHA1FileInfo]]] = Future.sequence(sha1FutureList)
    val s: List[SHA1FileInfo] = Await.result(xs2, Duration.Inf).flatten
    println("s:" + s.size)
    //files.foreach(x => Await.result(x, Duration.Inf))
    println("Got list time:" + ((System.currentTimeMillis - startTime) / 1000.0))
    updateFileIndex(s)
    println("End time:" + ((System.currentTimeMillis - startTime) / 1000.0))
  }

}

object HelloWorld {
  def main(args: Array[String]) {
    val t = new FMMasterDirActor("/mnt/WinD/MEGA")
    t.updateFileIndex()

  }
}
