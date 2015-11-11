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

/**
 *  This class represent a directory that should be
 *  managed and monitored by FMMaster. 
 *
 *  @param  dirLocation   The directory that will be watched.
 */
class FMMasterDirectory(dirLocation: String) {

  implicit val dirPath = Paths.get(dirLocation)
  implicit val dataSource = DatabaseDSL.createConnectionPool(dirPath)

  private val databaseFiles = DatabaseDSL.databaseFiles(dirPath)
  private val watcher = new JPathWatcherFileChangeMonitor(Paths.get(dirLocation))
  private val updateDBActor = UpdateDBActor.actorSystem.actorOf(Props(classOf[UpdateDBActor], dirLocation))

  /**
   *  Filter all files that should be updated in FileIndex table.
   *
   *  This method will check each file in basicFileInfoList, if
   *  file's legnth / lastModifiedTime differs from record in
   *  FileIndex, it means if should be updated.
   *
   *  @param    basicFileInfoList   The file list to be processed.
   *  @return                       Files in basicFileInfoList that need updated in FileIndexTable.
   */
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

  /**
   *  Update record in FileIndex table
   *
   *  This method will calculate new SHA1 hash checksum of files
   *  in fileList, and insert or update it's record in FileIndex
   *  table.
   *
   *  @param    fileList    Files need to updated in FileIndex.
   */
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

  /**
   *  Get all files in dirLocation.
   *
   *  Get all files in dirLocation recursively. (Include subdirectory)
   *
   *  @return   All files in dirLocation.
   */
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

  /**
   *  Update FileIndex table.
   *
   *  This method will scan through dirLocation and filter out
   *  files need to update, and update corresponding records
   *  in FileIndex table.
   */
  def updateFileIndex() {
    val files: List[BasicFileInfo] = getFileList
    val shouldUpdateList: List[BasicFileInfo] = getShouldUpdateFiles(files)
    updateFileIndex(shouldUpdateList)
  }

  /**
   *  Check if a Path object represent a directory
   *
   *  @param    path      Path want to check
   *  @return             True if path is a directory, false otherwise.
   */
  def isDirectory(path: Path) = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)

  /**
   *  Check if a Path object represent a FMMaster database releated file.
   *
   *  @param    path      Path want to check
   *  @return             True if path is a FMMaster database file, false otherwise.
   */
  def isNotDatabaseFile(path: Path) = !databaseFiles.contains(path.toAbsolutePath.toString)

  /**
   *  Start watching this directory.
   *
   *  This method will starts a watch service that monitors the file or directory
   *  changes in this directory, and pass message to UpdateDB actor to update 
   *  records in FMMaster database.
   */
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
        updateDBActor ! InsertIntoFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Modify(path) if isNotDatabaseFile(path) => 
        updateDBActor ! UpdateFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Delete(path) if isNotDatabaseFile(path) => 
        updateDBActor ! DeleteFromFileIndex(path, System.currentTimeMillis)
      case FileChangeMonitor.Rename(source, dest) if isNotDatabaseFile(source) && isNotDatabaseFile(dest) => 
        updateDBActor ! DeleteFromFileIndex(dest, System.currentTimeMillis)
        updateDBActor ! InsertIntoFileIndex(source, System.currentTimeMillis)
    }
  }

  /**
   *  Stop watching this directory.
   */
  def stopWatching() {
    watcher.stopWatch()
  }

}
