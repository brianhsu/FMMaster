package moe.brianhsu.fmmaster.utils

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileSystems
import java.nio.file.FileVisitResult
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import scala.collection.JavaConversions._

object FileChangeMonitor {

  /**
   *  The interface represent file change event.
   */
  sealed trait Event

  /**
   *  File or directory has been created.
   *
   *  @param  path  The path object that represented the created file
   */
  case class Create(path: Path) extends Event

  /**
   *  File or directory has been modified.
   *
   *  @param  path  The path object that represented the modified file
   */
  case class Modify(path: Path) extends Event

  /**
   *  File or directory has been deleted.
   *
   *  @param  path  The path object that represented the deleted file
   */
  case class Delete(path: Path) extends Event

  /**
   *  File or directory has been renamed.
   *
   *  @param  oldPath  The path object that represented the original file
   *  @param  newPath  The path object that represented the destination file
   */
  case class Rename(oldPath: Path, newPath: Path) extends Event
}

abstract class FileChangeMonitor(directory: Path, isRecursive: Boolean) {
  def startWatch[T](callback: PartialFunction[FileChangeMonitor.Event, T]): Unit
  def stopWatch(): Unit
}

/**
 *  This class will monitor a directory, and run the callback when
 *  file or folder is beeen created / modified / deleted or renamed.
 *
 *
 *  Example Usage:
 *
 *  {{{
      import moe.brianhsu.fmmaster.utils.FileChangeMonitor
      import java.nio.file.Paths
      val monitor = new JavaFileChangeMonitor(Paths.get("/home/brianhsu"), true)
      monitor.startWatch { 
        case FileChangeMonitor.Create(path) => println(s"File $path is created")
        case FileChangeMonitor.Rename(oldPath, newPath) => println("File $oldPath is renamed to $newPath")
      }
      println("This line will be print immediately")
      monitor.stopWatch()

 *  }}}
 *
 *  @param    directory     The directory been monitored.
 *  @param    isRecursive   Should we monitor subdirectory?
 */
class JavaFileChangeMonitor(directory: Path, isRecursive: Boolean = true) extends FileChangeMonitor(directory, isRecursive) {

  import FileChangeMonitor._

  private var watcher = FileSystems.getDefault.newWatchService
  private var keyToDirectory: Map[WatchKey, Path] = Map.empty
  private var shouldStop: Boolean = false
  private var watchThreadHolder: Option[Thread] = None

  /**
   *  Register a directory to WatcherService
   *
   *  @param  directory   The directory been monitored.
   */
  private def register(directory: Path) {
    val watchKey = directory.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)
    println(s"Register $directory....")
    keyToDirectory += (watchKey -> directory)
  }

  /**
   *  Register a directory and all of it subdirectory to WatcherService.
   *
   *  @param  directory   The directory been monitored.
   */
  private def registerAll(directory: Path) {
    class FileLister extends SimpleFileVisitor[Path] {
      override def preVisitDirectory(directory: Path, attr: BasicFileAttributes): FileVisitResult = {
        register(directory)
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(directory, new FileLister)
  }

  /**
   *  Create a thread that wait events from WatcherService, and call appropriate function in
   *  callback.
   *
   *  @param  callback    The callback describe corresponding action for each event.
   */
  private def createWatchThread[T](callback: PartialFunction[FileChangeMonitor.Event, T]) = new Thread {

    private def processEvent(event: WatchEvent[Path], eventContext: Path, currentDirectory: Path) {
      val eventKind = event.kind
      val eventPath = currentDirectory.resolve(eventContext).toAbsolutePath
      val isDirectory = Files.isDirectory(eventPath, LinkOption.NOFOLLOW_LINKS)

      if (isRecursive && eventKind == ENTRY_CREATE && isDirectory) {
        registerAll(eventPath)
      }

      val callbackData = eventKind match {
        case ENTRY_CREATE => FileChangeMonitor.Create(eventPath)
        case ENTRY_MODIFY => FileChangeMonitor.Modify(eventPath)
        case ENTRY_DELETE => FileChangeMonitor.Delete(eventPath)
      }

      if (callback.isDefinedAt(callbackData)) {
        callback(callbackData)
      }
    }

    private def processEventList(watchKey: WatchKey, currentDirectory: Path) {

      val eventList = watchKey.pollEvents.toList.map(_.asInstanceOf[WatchEvent[Path]])

      for {
        event <- eventList
      } {
        processEvent(event, event.context, currentDirectory)
      }

      val isValid = watchKey.reset

      if (!isValid) {
        keyToDirectory -= watchKey
      }
    }

    override def run() {
      isRecursive match {
        case true  => registerAll(directory)
        case false => register(directory)
      }

      while (!shouldStop) {
        for {
          watchKey <- scala.util.Try(watcher.take).toOption
          currentDirectory <- keyToDirectory.get(watchKey)
        } {
          processEventList(watchKey, currentDirectory)
        }
      }
    }
  }

  /**
   *  Starts the file change monitor.
   *
   *  This will starts the monitor thread. When a event occurred, it will call
   *  the callback function to handle that event.
   *
   *  @param  callback    The callback describe corresponding action for each event.
   */
  def startWatch[T](callback: PartialFunction[FileChangeMonitor.Event, T]) {
    shouldStop = false
    watcher = FileSystems.getDefault.newWatchService
    watchThreadHolder = Some(createWatchThread(callback))
    watchThreadHolder.foreach(_.start())
  }

  /**
   *  Stop the watcher thread.
   *
   *  It will block until the thread is terminated.
   */
  def stopWatch() {
    shouldStop = true
    watcher.close()
    watchThreadHolder.foreach(_.join())
  }


}
