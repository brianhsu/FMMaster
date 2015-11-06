package moe.brianhsu.fmmaster.utils

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.LinkOption
import java.nio.file.SimpleFileVisitor
import java.nio.file.{Path => JPath, Paths => JPaths}
import name.pachler.nio.file._
import name.pachler.nio.file.ext.ExtendedWatchEventKind._
import name.pachler.nio.file.StandardWatchEventKind._
import scala.collection.JavaConversions._

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
      val monitor = new JPathWatchFileChangeMonitor(Paths.get("/home/brianhsu"), true)
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
class JPathWatcherFileChangeMonitor(directory: JPath, isRecursive: Boolean = true) extends FileChangeMonitor(directory, isRecursive) {

  import FileChangeMonitor._

  private var watcher = FileSystems.getDefault.newWatchService
  private var keyToDirectory: Map[WatchKey, Path] = Map.empty
  private var shouldStop: Boolean = false
  private var watchThreadHolder: Option[Thread] = None
  private def toJavaPath(path: Path) = JPaths.get(path.toString)
  private def toWatcherPath(path: JPath) = Paths.get(path.toAbsolutePath.toString)

  /**
   *  Register a directory to WatcherService
   *
   *  @param  directory   The directory been monitored.
   */
  private def register(directory: Path) {
    val watchKey = directory.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY, ENTRY_RENAME_FROM, ENTRY_RENAME_TO)
    keyToDirectory += (watchKey -> directory)
  }

  /**
   *  Register a directory and all of it subdirectory to WatcherService.
   *
   *  @param  directory   The directory been monitored.
   */
  private def registerAll(directory: Path) {
    class FileLister extends SimpleFileVisitor[JPath] {
      override def preVisitDirectory(directory: JPath, attr: BasicFileAttributes): FileVisitResult = {
        register(toWatcherPath(directory))
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(toJavaPath(directory), new FileLister)
  }

  /**
   *  Create a thread that wait events from WatcherService, and call appropriate function in
   *  callback.
   *
   *  @param  callback    The callback describe corresponding action for each event.
   */
  private def createWatchThread[T](callback: PartialFunction[FileChangeMonitor.Event, T]) = new Thread {

    private var renameFromHolder: Option[JPath] = None

    private def processEvent(event: WatchEvent[Path], eventContext: JPath, currentDirectory: Path) {
      val eventKind = event.kind
      val eventPath = toJavaPath(currentDirectory).resolve(eventContext).toAbsolutePath
      val isDirectory = Files.isDirectory(eventPath, LinkOption.NOFOLLOW_LINKS)

      if (isRecursive && eventKind == ENTRY_CREATE && isDirectory) {
        registerAll(toWatcherPath(eventPath))
      }

      if (eventKind == ENTRY_RENAME_FROM) {
        renameFromHolder = Some(eventPath)
      } else {

        val RENAME_TO = ENTRY_RENAME_TO
        val callbackData = eventKind match {
          case ENTRY_CREATE => FileChangeMonitor.Create(eventPath)
          case ENTRY_MODIFY => FileChangeMonitor.Modify(eventPath)
          case ENTRY_DELETE => FileChangeMonitor.Delete(eventPath)
          case RENAME_TO    => FileChangeMonitor.Rename(renameFromHolder.get, eventPath)
        }

        if (callback.isDefinedAt(callbackData)) {
          callback(callbackData)
        }
      }
    }

    private def processEventList(watchKey: WatchKey, currentDirectory: Path) {

      val eventList = watchKey.pollEvents.toList.map(_.asInstanceOf[WatchEvent[Path]])

      for {
        event <- eventList
        eventContext <- scala.util.Try(toJavaPath(event.context))
      } {
        processEvent(event, eventContext, currentDirectory)
      }

      val isValid = watchKey.reset

      if (!isValid) {
        keyToDirectory -= watchKey
      }
    }

    override def run() {
      isRecursive match {
        case true  => registerAll(toWatcherPath(directory))
        case false => register(toWatcherPath(directory))
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
