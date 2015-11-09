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

  implicit val dirPath = Paths.get(dirLocation)
  implicit val dataSource = DatabaseDSL.createConnectionPool(dirPath)

  def receive = {
    case x => println(s"===> Receive: $x")
  }

}
