package moe.brianhsu.fmmaster

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import moe.brianhsu.fmmaster.model._
import moe.brianhsu.fmmaster.model.DatabaseDSL
import moe.brianhsu.fmmaster.model.DatabaseDSL._
import moe.brianhsu.fmmaster.utils._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FMMasterDirectory(dirLocation: String) {

  implicit val dirPath = Paths.get(dirLocation)
  implicit val dataSource = DatabaseDSL.createConnectionPool(dirPath)

  val watcher = new JavaFileChangeMonitor(Paths.get(dirLocation))

  def getShouldUpdateFiles(basicFileInfoList: List[BasicFileInfo]) =  {
    using(dataSource) {
      basicFileInfoList.filter { basicFileInfo =>
        val file = FMMasterDirDB.fileIndex.where(_.filePath === basicFileInfo.filePath).headOption
        val filteredFile = file.filter { f => 
          f.size == basicFileInfo.size && f.lastModifiedTime == basicFileInfo.lastModifiedTime
        }
        filteredFile.isEmpty
      }
    }
  }

  def updateFileIndex(fileList: List[SHA1FileInfo]) = Future {
    using(dataSource) {
      fileList.foreach { sha1FileInfo =>
        val basicFileInfo = sha1FileInfo.basicFileInfo
        if (basicFileInfo.file.exists) {
          val fileHolder = FMMasterDirDB.fileIndex.where(_.filePath === basicFileInfo.filePath).headOption
          fileHolder match {
            case None =>
              FMMasterDirDB.fileIndex.insert(
                new FileIndex(sha1FileInfo.sha1, basicFileInfo.filePath, basicFileInfo.size, basicFileInfo.lastModifiedTime)
              )
            case Some(file) =>
              file.sha1 = sha1FileInfo.sha1
              file.size = basicFileInfo.size
              file.lastModifiedTime = basicFileInfo.lastModifiedTime
              FMMasterDirDB.fileIndex.update(file)
          }
        }
      }
    }
  }

  def getSHA1FileInfo(basicFileInfo: BasicFileInfo): Future[Option[SHA1FileInfo]] = {
    Future(SHA1Utils.sha1Checksum(basicFileInfo.file)).
      map(sha1 => Some(SHA1FileInfo(basicFileInfo, sha1))).
      recover { case e: Exception => None }
  }

  def getFileList: List[BasicFileInfo] = {

    var files: List[BasicFileInfo] = Nil

    class FileLister extends SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
        val relativePath = dirPath.relativize(file)
        val basicFileInfo = BasicFileInfo(relativePath.normalize.toString, attr.size, attr.lastModifiedTime.toMillis)
        files ::= basicFileInfo
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(dirPath, new FileLister)
    files
  }

  def updateFileIndex() {
    val startTime = System.currentTimeMillis
    val files: List[BasicFileInfo] = getFileList
    val shouldUpdateList: List[BasicFileInfo] = getShouldUpdateFiles(files)
    val sha1FutureList = Future.sequence(shouldUpdateList.map(getSHA1FileInfo))
    val sha1List: List[SHA1FileInfo] = Await.result(sha1FutureList, Duration.Inf).flatten
    updateFileIndex(sha1List)
  }

  def startWatching() {
    watcher.startWatch {
      case FileChangeMonitor.Create(t) => println("File Create:" + t)
      case FileChangeMonitor.Modify(t) => println("File Modify:" + t)
      case FileChangeMonitor.Delete(t) => println("File Delete:" + t)
      case FileChangeMonitor.Rename(t, s) => println("File Rename:" + t + " -> " + s)
    }
  }

  def stopWatching() {
    watcher.stopWatch()
  }

}
