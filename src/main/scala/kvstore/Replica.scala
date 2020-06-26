package kvstore

import akka.actor.{Actor, ActorRef, Props, Terminated}
import kvstore.Arbiter._

import scala.concurrent.duration._

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

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var pendingPersistances = Map.empty[Long, PendingPersistance]

  case class PendingPersistance(sender: ActorRef, persist: Persist, var retries: Int, persisted: Boolean, remainingReplicationAcks: Int)

  var seqNumber = 0L
  private val persistence: ActorRef = context.actorOf(persistenceProps)

  context.system.scheduler.scheduleWithFixedDelay(100.millis, 100.millis) {
    new Runnable {
      override def run(): Unit = {
        pendingPersistances.foreach { entry =>
          val persistanceEntry = entry._2

          if (persistanceEntry.retries > 10) {
            val seqNumber = entry._1
            //            pendingPersistances = pendingPersistances.removed(seqNumber)
            persistanceEntry.sender ! OperationFailed(seqNumber)
          } else {
            persistence ! persistanceEntry.persist
            persistanceEntry.retries += 1
          }
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
      val persist = Persist(key, Some(value), id)
      pendingPersistances += (id -> PendingPersistance(sender, persist, 0, false, secondaries.size))
      persistence ! persist
      secondaries.foreach(secondary => secondary._2 ! Replicate(key, Some(value), id))
    case Get(key, id) =>
      val maybeValue = kv.get(key)
      sender ! GetResult(key, maybeValue, id)
    case Remove(key, id) =>
      kv = kv.removed(key)
      secondaries.foreach(secondary => secondary._2 ! Replicate(key, None, id))
      sender ! OperationAck(id)
    case Persisted(_, id) =>
      pendingPersistances = pendingPersistances.get(id) match {
        case Some(persistanceEntry) =>
          if (persistanceEntry.remainingReplicationAcks == 0) {
            persistanceEntry.sender ! OperationAck(id)
            pendingPersistances.removed(id)
          } else {
            pendingPersistances.updated(id, persistanceEntry.copy(persisted = true))
          }
        case _ =>
          pendingPersistances
      }
    case Replicas(replicas) =>
      secondaries.filterNot{r =>
        replicas.exists(_.path == r._1.path) == true}.foreach{r =>
        context.stop(r._2)}
      replicators = Set.empty
      secondaries = Map.empty
      replicas.foreach(replica => {
        val replicator = context.actorOf(Replicator.props(replica))
        replicators += replicator
        if (replica != this.context.self) {
          kv.zipWithIndex.foreach {
            case (v, i) => replicator ! Replicate(v._1, Some(v._2), i)
          }
          secondaries += (replica -> replicator)
        }
      })
    case Replicated(_, seq) =>
      pendingPersistances = pendingPersistances.get(seq) match {
        case Some(persistanceEntry) =>
          if (persistanceEntry.persisted == true && persistanceEntry.remainingReplicationAcks == 1) {
            persistanceEntry.sender ! OperationAck(seq)
            pendingPersistances.removed(seq)
          } else {
            val remainingAcks = persistanceEntry.remainingReplicationAcks
            pendingPersistances.updated(seq, persistanceEntry.copy(remainingReplicationAcks = remainingAcks - 1))
          }
        case _ => pendingPersistances
      }
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
      pendingPersistances += (seq -> PendingPersistance(sender, persist, 0, false, 0))
      persistence ! persist
    case Snapshot(key, _, seq) if seq < seqNumber =>
      sender ! SnapshotAck(key, seq)
    case Persisted(key, id) =>
      pendingPersistances = pendingPersistances.get(id) match {
        case Some(persistanceEntry) =>
          seqNumber += 1L
          persistanceEntry.sender ! SnapshotAck(key, id)
          pendingPersistances.removed(id)
        case _ => pendingPersistances
      }
  }

  arbiter ! Join

}

