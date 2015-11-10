package moe.brianhsu.fmmaster

import akka.actor.Props
import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.LinkOption
import java.nio.file.SimpleFileVisitor
import moe.brianhsu.fmmaster.actor._
import moe.brianhsu.fmmaster.actor.UpdateDBActor._
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

  private val databaseFiles = DatabaseDSL.databaseFiles(dirPath)
  private val watcher = new JPathWatcherFileChangeMonitor(Paths.get(dirLocation))
  private val updateDBActor = UpdateDBActor.actorSystem.actorOf(Props(classOf[UpdateDBActor], dirLocation))

  def getShouldUpdateFiles(basicFileInfoList: List[BasicFileInfo]) =  {
    using(dataSource) {
      basicFileInfoList.filter { basicFileInfo =>
        val relativePath = dirPath.relativize(basicFileInfo.filePath).toString
        val file = FMMasterDirDB.fileIndex.where(_.filePath === relativePath).headOption
        val filteredFile = file.filter { f => 
          f.size == basicFileInfo.size && f.lastModifiedTime == basicFileInfo.lastModifiedTime
        }
        filteredFile.isEmpty
      }
    }
  }

  def updateFileIndex(fileList: List[BasicFileInfo]) = Future {
    using(dataSource) {
      fileList.foreach { basicFileInfo =>
        if (basicFileInfo.file.exists) {
          val relativePath = dirPath.relativize(basicFileInfo.filePath).toString
          val fileHolder = FMMasterDirDB.fileIndex.where(_.filePath === relativePath).headOption
          val message = fileHolder match {
            case None => InsertIntoFileIndex(basicFileInfo.filePath, System.currentTimeMillis)
            case Some(file) => UpdateFileIndex(basicFileInfo.filePath, System.currentTimeMillis)
          }
          println(s"====> Sent message: $message")
          updateDBActor ! message
        }
      }
    }
  }

  def getFileList: List[BasicFileInfo] = {

    var files: List[BasicFileInfo] = Nil

    class FileLister extends SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
        val basicFileInfo = BasicFileInfo(file, attr.size, attr.lastModifiedTime.toMillis)
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
    updateFileIndex(shouldUpdateList)
  }

  def isDirectory(path: Path) = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
  def isNotDatabaseFile(path: Path) = !databaseFiles.contains(path.toAbsolutePath.toString)

  def startWatching() {
    watcher.startWatch {
      case FileChangeMonitor.Create(path) if isDirectory(path) => 
        updateDBActor ! InsertDirectoryIntoFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Delete(path) if isDirectory(path) => 
        updateDBActor ! DeleteDirectoryFromFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Rename(source, dest) if isDirectory(dest) => 
        updateDBActor ! InsertDirectoryIntoFileIndex(dest, System.currentTimeMillis)
        updateDBActor ! DeleteDirectoryFromFileIndex(source, System.currentTimeMillis)

      case FileChangeMonitor.Create(path) if isNotDatabaseFile(path) => 
        println(s"FileChangeMonitor.Create($path)")
        updateDBActor ! InsertIntoFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Modify(path) if isNotDatabaseFile(path) => 
        println(s"FileChangeMonitor.Modify($path)")
        updateDBActor ! UpdateFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Delete(path) if isNotDatabaseFile(path) => 
        println(s"FileChangeMonitor.Delete($path)")
        updateDBActor ! DeleteFromFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Rename(source, dest) if isNotDatabaseFile(source) && isNotDatabaseFile(dest) => 
        println(s"FileChangeMonitor.Rename($source, $dest)")
        updateDBActor ! DeleteFromFileIndex(dest, System.currentTimeMillis)
        updateDBActor ! InsertIntoFileIndex(source, System.currentTimeMillis)
    }
  }

  def stopWatching() {
    watcher.stopWatch()
  }

}
