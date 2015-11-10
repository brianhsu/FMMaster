package moe.brianhsu.fmmaster.actor

import akka.actor._
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import moe.brianhsu.fmmaster.model.DatabaseDSL
import moe.brianhsu.fmmaster.model.DatabaseDSL._
import moe.brianhsu.fmmaster.utils._

object UpdateDBActor {

  lazy val actorSystem = ActorSystem("defaultActorSystem")

  sealed trait Message

  case class InsertIntoFileIndex(path: Path, timestamp: Long) extends Message
  case class DeleteFromFileIndex(path: Path, timstamp: Long) extends Message
  case class UpdateFileIndex(path: Path, timestamp: Long) extends Message
  case class InsertDirectoryIntoFileIndex(path: Path, timestamp: Long) extends Message
  case class DeleteDirectoryFromFileIndex(path: Path, timstamp: Long) extends Message
  case class UpdateDirectoryFileIndex(path: Path, timestamp: Long) extends Message
}

class UpdateDBActor(dirLocation: String) extends Actor {

  import UpdateDBActor._

  implicit val dirPath = Paths.get(dirLocation)
  implicit val dataSource = DatabaseDSL.createConnectionPool(dirPath)

  case class Metadata(fieldName: String, fieldType: String)

  def relativePath(path: Path) = dirPath.relativize(path).toString

  def getMetadataList(path: Path): List[MetadataStore] = {
    val filePath = relativePath(path)
    val sha1 = FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.map(_.sha1)

    for {
      sha1     <- FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.map(_.sha1)
      metadata <- FMMasterDirDB.metadataStore.where(_.sha1 === sha1).toList
    } yield metadata
  }

  def getIndexCountByPath(filePath: String) = {
    val query = 
      from(FMMasterDirDB.fileIndex) { row => 
        where(row.filePath === filePath).
        compute(count)
      }

    query.head.measures
  }

  def getDeletedIndex(filePath: String) = FMMasterDirDB.deletedIndex.where(_.filePath === filePath).headOption

  def getIndexCountBySHA1(sha1: String) = {
    val query = 
      from(FMMasterDirDB.fileIndex) { row => 
        where(row.sha1 === sha1).
        compute(count)
      }

    query.head.measures
  }

  def deleteMetadataStore(sha1: String) = { FMMasterDirDB.metadataStore.deleteWhere(_.sha1 === sha1) }
  def deleteFileIndex(filePath: Path) = { FMMasterDirDB.fileIndex.deleteWhere(_.filePath === relativePath(filePath)) }

  def insertIntoFileIndex(fullPath: Path, sha1: String) {
    val filePath = relativePath(fullPath)
    val attrs = Files.readAttributes(fullPath, classOf[BasicFileAttributes])
    val newFileIndex = new FileIndex(sha1, filePath, attrs.size, attrs.lastModifiedTime.toMillis)
    FMMasterDirDB.fileIndex.insert(newFileIndex)
    compactDeletedIndex(fullPath)
  }

  def insertIntoMetadataStore(sha1: String, title: String, content: String) {
    FMMasterDirDB.metadataStore.insert(new MetadataStore(sha1, title, content))
  }

  def updateFileIndex(fullPath: Path, sha1: String) {
    val filePath = relativePath(fullPath)
    val attrs = Files.readAttributes(fullPath, classOf[BasicFileAttributes])
    val metadataList = getMetadataList(fullPath).filter(_.sha1 != sha1)
    deleteFileIndex(fullPath)
    insertIntoFileIndex(fullPath, sha1)
    metadataList.foreach { record => insertIntoMetadataStore(sha1, record.fieldName, record.content) }
    val shouldDeletedMetadataSHA1List = metadataList.map(_.sha1).distinct.filter(sha1 => getIndexCountBySHA1(sha1) == 0)
    shouldDeletedMetadataSHA1List.foreach(deleteMetadataStore)
    compactDeletedIndex(fullPath)
  }

  def processInsertEvent(fullPath: Path, timestamp: Long) {
    val sha1 = SHA1Utils.sha1Checksum(fullPath.toString)
    val filePath = relativePath(fullPath)
    val isFileIndexDeleted = getDeletedIndex(filePath)
    val isFileIndexed = getIndexCountByPath(filePath) > 0

    def isEditingFile(deletedTimestamp: Long) = {
      timestamp - deletedTimestamp >= 0 &&
      timestamp - deletedTimestamp <= 1000
    }

    isFileIndexDeleted match {
      case None if !isFileIndexed => insertIntoFileIndex(fullPath, sha1)  // Brand new file, just insert into FileIndex table
      case None if isFileIndexed  => updateFileIndex(fullPath, sha1)      // File edition, we need copy metadata to new SHA1 hash
      case Some(deletedFile) if isEditingFile(deletedFile.deletedTime) => updateFileIndex(fullPath, sha1)
      case Some(deletedFile) => insertIntoFileIndex(fullPath, sha1)
    }
  }

  def processDeleteEvent(fullPath: Path, timestamp: Long) {
    val filePath = relativePath(fullPath)
    compactDeletedIndex(fullPath)
    FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.foreach { oldFileIndex =>
      FMMasterDirDB.deletedIndex.insert(new DeletedIndex(oldFileIndex.sha1, filePath, timestamp))
    }
    FMMasterDirDB.fileIndex.deleteWhere(_.filePath === filePath)
  }

  def compactDeletedIndex(fullPath: Path) {
    val filePath = relativePath(fullPath)
    FMMasterDirDB.deletedIndex.deleteWhere(_.filePath === filePath)
  }


  def receive = {
    case InsertIntoFileIndex(path, timestamp) => using(dataSource) { processInsertEvent(path, timestamp) }
    case UpdateFileIndex(path, timestamp) => using(dataSource) { updateFileIndex(path, SHA1Utils.sha1Checksum(path.toString)) }
    case DeleteFromFileIndex(path, timestamp) => using(dataSource) { processDeleteEvent(path, timestamp) }
    case x => println(s"===> Receive: $x")
  }

}
