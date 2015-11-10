package moe.brianhsu.fmmaster.actor

import akka.actor._
import java.nio.file.Path
import java.nio.file.Paths
import moe.brianhsu.fmmaster.model.DatabaseDSL
import moe.brianhsu.fmmaster.model.DatabaseDSL._

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

  def getMetadata(path: Path): List[MetadataStore] = {
    val filePath = relativePath(path)
    val sha1 = FMMasterDirDB.fileIndex.where(_.filePath === filePath).headOption.map(_.sha1)

    for {
      sha1     <- FMMasterDirDB.fileIndex.where(_.filePath === filePath).toList.map(_.sha1)
      metadata <- FMMasterDirDB.metadataStore.where(_.sha1 === sha1).toList
    } yield metadata
  }


  def getSHA1FileIndexCount(sha1: String) = {
    val query = 
      from(FMMasterDirDB.fileIndex) { row => 
        where(row.sha1 === sha1).
        compute(count)
      }

    query.head.measures
  }

  def deleteMetadataStore(sha1: String) = { FMMasterDirDB.metadataStore.deleteWhere(_.sha1 === sha1) }
  def deleteFileIndex(filePath: Path) = { FMMasterDirDB.fileIndex.deleteWhere(_.filePath === relativePath(filePath)) }

  def receive = {
    case InsertIntoFileIndex(path, timestamp) => using(dataSource) { println(getMetadata(path)) }
    case x => println(s"===> Receive: $x")
  }

}
