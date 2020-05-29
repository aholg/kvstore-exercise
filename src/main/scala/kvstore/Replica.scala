package kvstore

import akka.actor.{OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var pendingPersistances = Map.empty[Long, (ActorRef, Persist)]

  var seqNumber = 0L
  private val persistence: ActorRef = context.actorOf(persistenceProps)

  context.system.scheduler.scheduleWithFixedDelay(100.millis, 100.millis) {
    new Runnable {
      override def run(): Unit = {
        pendingPersistances.foreach { entry =>
          val persistanceEntry = entry._2._2
          persistence ! persistanceEntry
        }
      }
    }
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      sender ! OperationAck(id)
    case Get(key, id) =>
      val maybeValue = kv.get(key)
      sender ! GetResult(key, maybeValue, id)
    case Remove(key, id) =>
      kv = kv.removed(key)
      sender ! OperationAck(id)
  }

  val replica: Receive = {
    case Get(key, id) =>
      val maybeValue = kv.get(key)
      sender ! GetResult(key, maybeValue, id)
    case Snapshot(key, maybeValue, seq) if seq == seqNumber =>
      maybeValue match {
        case Some(value) => kv = kv.updated(key, value)
        case None => kv = kv.removed(key)
      }
      val persist = Persist(key, maybeValue, seq)
      pendingPersistances += (seq -> (sender, persist))

      persistence ! persist
    case Snapshot(key, _, seq) if seq < seqNumber =>
      sender ! SnapshotAck(key, seq)
    case Persisted(key, id) =>
      pendingPersistances = pendingPersistances.get(id) match {
        case Some(persistanceEntry) =>
          seqNumber += 1L
          persistanceEntry._1 ! SnapshotAck(key, id)
          pendingPersistances.removed(id)
        case _ => pendingPersistances
      }
  }

  arbiter ! Join

}

