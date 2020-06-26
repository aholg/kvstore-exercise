package kvstore

import akka.actor.{Actor, ActorRef, Props, Terminated}

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher
  import scala.concurrent.duration._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  context.system.scheduler.scheduleWithFixedDelay(100.millis, 100.millis) {
    new Runnable {
      override def run(): Unit = {
        acks.foreach { entry =>
          val replicateMsg = entry._2._2
          replica ! Snapshot(replicateMsg.key, replicateMsg.valueOption, replicateMsg.id)
        }
      }
    }
  }

  def receive: Receive = {
    case replicateMsg @Replicate(key, maybeValue, seq) =>
      acks += (seq -> (sender, replicateMsg))
      replica ! Snapshot(key, maybeValue, seq)
    case SnapshotAck(key, seq) =>
      acks = acks.get(seq) match {
        case Some(replicateMsg) =>
          replicateMsg._1 ! Replicated(key, seq)
          acks.removed(seq)
        case _ => acks
      }
  }

  override def postStop(){
    acks.foreach { ack =>
      val replicateMsg = ack._2
      replicateMsg._1 ! Replicated(replicateMsg._2.key, replicateMsg._2.id)
    }
  }
}
