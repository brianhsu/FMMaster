package moe.brianhsu.fmmaster.actor

import akka.actor._
import java.nio.file.Path

object UpdateDBActor {
  lazy val actorSystem = ActorSystem("defaultActorSystem")

  sealed trait Message

  case class InsertToFileIndex(path: Path) extends Message
  case class DeleteFromFileIndex(path: Path) extends Message
  case class UpdateFileIndex(path: Path) extends Message

}

class UpdateDBActor extends Actor {

  def receive = {
    case x => println(s"receive: $x")
  }

}
