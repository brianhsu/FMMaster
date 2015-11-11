package moe.brianhsu.fmmaster.actor

import akka.actor._
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
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
  case object CompactMetadataStore
}

class UpdateDBActor(dirLocation: String) extends Actor {

  import UpdateDBActor._

  implicit val dirPath = Paths.get(dirLocation)
  implicit val dataSource = DatabaseDSL.createConnectionPool(dirPath)

  /**
   *  Get relative path of files compare to dirLocation.
   *
   *  @param    path    The file path.
   *  @return           The relativize path string.
   */
  def relativePath(path: Path) = dirPath.relativize(path).toString

  /**
   *  Get metdata list of a file.
   *
   *  @param    path    The file path want to query.
   *  @return           The list of metadata associated with this file.
   */
  def getMetadataList(path: Path): List[MetadataStore] = {
    val filePath = relativePath(path)
    val sha1 = FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.map(_.sha1)

    for {
      sha1     <- FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.map(_.sha1)
      metadata <- FMMasterDirDB.metadataStore.where(_.sha1 === sha1).toList
    } yield metadata
  }

  /**
   *  Get how many record exist in FileIndex for a file
   *
   *  @param    filePath    The file path to be queried.
   *  @return               Record counts for this file in FileIndex table.
   */
  def getIndexCountByPath(filePath: String) = {
    val query = 
      from(FMMasterDirDB.fileIndex) { row => 
        where(row.filePath === filePath).
        compute(count)
      }

    query.head.measures
  }

  /**
   *  Get how many record exist in DeletedIndex for a file
   *
   *  @param    filePath    The file path to be queried.
   *  @return               Record counts for this file in DeletedIndex table.
   */
  def getDeletedIndex(filePath: String) = FMMasterDirDB.deletedIndex.where(_.filePath === filePath).headOption

  /**
   *  Get how many record exist in FileIndex for a certain SHA1 hash.
   *
   *  @param    sha1    The SHA1 hash value to be queried.
   *  @return           Record counts for this file in FileIndex table.
   */
  def getIndexCountBySHA1(sha1: String) = {
    val query = 
      from(FMMasterDirDB.fileIndex) { row => 
        where(row.sha1 === sha1).
        compute(count)
      }

    query.head.measures
  }

  /**
   *  Delete metadata of SHA1 value.
   *
   *  @param    sha1    Delete metadata list of certain SHA1 hash value.
   */
  def deleteMetadataStore(sha1: String) = { FMMasterDirDB.metadataStore.deleteWhere(_.sha1 === sha1) }

  /**
   *  Delete records that match filePath in FileIndex table.
   *
   *  @param  filePath    The file path need to be deleted.
   */
  def deleteFileIndex(filePath: Path) = { FMMasterDirDB.fileIndex.deleteWhere(_.filePath === relativePath(filePath)) }

  /**
   *  Insert a file path into FileIndex and compact DeletedIndex table.
   *
   *  This method will insert fullPath into FileIndex table, and deleted
   *  any corresponding records in DeleteIndex table.
   *
   *  @param  fullPath    The path to be inserted.
   *  @param  sha1        SHA1 hash value of the file represented by fullPath.
   */
  def insertIntoFileIndex(fullPath: Path, sha1: String) {
    val filePath = relativePath(fullPath)
    val attrs = Files.readAttributes(fullPath, classOf[BasicFileAttributes])
    val newFileIndex = new FileIndex(sha1, filePath, attrs.size, attrs.lastModifiedTime.toMillis)
    FMMasterDirDB.fileIndex.insert(newFileIndex)
    compactDeletedIndex(fullPath)
  }

  /**
   *  Insert metadata to corresponding SHA1 hash value
   *
   *  @param    sha1      The SHA1-hash of this metadata.
   *  @param    title     Metadata title.
   *  @param    content   Metadata content.
   */
  def insertIntoMetadataStore(sha1: String, title: String, content: String) {
    FMMasterDirDB.metadataStore.insert(new MetadataStore(sha1, title, content))
  }

  /**
   *  Update a file in FileIndex
   *
   *  This method will updated a file's SHA1 in FileIndex,
   *  and also updated MetadataStore so the latest SHA1
   *  hash value has same metadata as old one.
   *
   *  @param    fullPath    The path of file.
   *  @param    sha1        The new SHA1 value of updated file.
   */
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

  /**
   *  Process file create event.
   *
   *  @param    fullPath      The file been created.
   *  @param    timestamp     The timestamp of this event.
   */
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

  /**
   *  Process file deletion event.
   *
   *  This will move record in FileIndex to DeletedIndex.
   */
  def processDeleteEvent(fullPath: Path, timestamp: Long) {
    val filePath = relativePath(fullPath)
    compactDeletedIndex(fullPath)
    FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.foreach { oldFileIndex =>
      FMMasterDirDB.deletedIndex.insert(new DeletedIndex(oldFileIndex.sha1, filePath, timestamp))
    }
    FMMasterDirDB.fileIndex.deleteWhere(_.filePath === filePath)
  }

  /**
   *  Delete DeletedIndex record that files is not valid anymore. 
   *  
   *  @param  fullPath    The file to be deleted.
   */
  def compactDeletedIndex(fullPath: Path) {
    val filePath = relativePath(fullPath)
    FMMasterDirDB.deletedIndex.deleteWhere(_.filePath === filePath)
  }

  /**
   *  Add files of directory into FileIndex.
   *
   *  @param  fullPath    The directory to be inserted.
   *  @param  timestamp   Timestamp of this event.
   */
  def processDirectoryInsertEvent(fullPath: Path, timestamp: Long) {

    class FileLister extends SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
        self ! InsertIntoFileIndex(file.toAbsolutePath, timestamp)
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(fullPath, new FileLister)
    self ! CompactMetadataStore
  }

  /**
   *  Delete record in FileIndex that has certian path.
   *
   *  @param  fullPath    Path that need to check
   *  @param  timestamp   Timestamp of this event.
   */
  def processDirectoryDelete(fullPath: Path, timestamp: Long) {
    val filePath = relativePath(fullPath)
    val fileList = FMMasterDirDB.fileIndex.where(_.filePath like s"$filePath%").toList

    fileList.foreach { record =>
      val isFileNotExists = !fullPath.resolve(record.filePath).toFile.exists
      if (isFileNotExists) {
        FMMasterDirDB.fileIndex.deleteWhere(_.filePath === record.filePath)
      }
    }
  }

  /**
   *  Delete unused metadata in MetadataStore
   */
  def compactMetadataStore() = {
    val connection = dataSource.getConnection
    val statement = connection.createStatement
    statement.executeUpdate("""DELETE FROM MetadataStore WHERE sha1 NOT IN (SELECT sha1 FROM FileIndex)""")
    statement.close()
    connection.close()
  }

  def receive = {
    case InsertIntoFileIndex(path, timestamp) => using(dataSource) { processInsertEvent(path, timestamp) }
    case UpdateFileIndex(path, timestamp) => using(dataSource) { updateFileIndex(path, SHA1Utils.sha1Checksum(path.toString)) }
    case DeleteFromFileIndex(path, timestamp) => using(dataSource) { processDeleteEvent(path, timestamp) }
    case InsertDirectoryIntoFileIndex(path, timestamp) => 
      println(s"InsertDirectoryIntoFileIndex($path, $timestamp)")
      processDirectoryInsertEvent(path, timestamp)
    case DeleteDirectoryFromFileIndex(path, timestamp) => 
      println(s"DeleteDirectoryFromFileIndex($path, $timestamp)")
      using(dataSource) { processDirectoryDelete(path, timestamp) }
    case CompactMetadataStore => compactMetadataStore
  }

}
