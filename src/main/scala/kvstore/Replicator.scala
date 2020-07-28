package kvstore

import akka.actor.{Actor, ActorRef, Props}

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case class PendingSnapshot(id: Long, snapshot: Snapshot)

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
  var acks = Map.empty[Long, (ActorRef, PendingSnapshot)]
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
          val snapshot = entry._2._2.snapshot
          replica ! snapshot
        }
      }
    }
  }

  def receive: Receive = {
    case Replicate(key, maybeValue, id) =>
      val seq = nextSeq
      val snapshot = Snapshot(key, maybeValue, seq)
      acks += (seq -> (sender, PendingSnapshot(id, snapshot)))
      replica ! snapshot
    case s @SnapshotAck(key, seq) =>
      acks = acks.get(seq) match {
        case Some(replicateMsg) =>
          replicateMsg._1 ! Replicated(key, replicateMsg._2.id)
          acks.removed(seq)
        case _ =>
          acks
      }
  }

  override def postStop() {
    acks.foreach { ack =>
      val snapshot = ack._2
      snapshot._1 ! Replicated(snapshot._2.snapshot.key, ack._1)
    }
  }
}
