package me.atomd.things.db

import akka.actor._
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistenceFailure
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotOffer
import me.atomd.things.ThingExtension

import scala.concurrent.duration._

object PersistentThing {

  def props(): Props = Props(classOf[PersistentThing])

  case object SaveSnapshot
}

final class PersistentThing extends Thing with PersistentActor with ActorLogging {

  import context.dispatcher
  import me.atomd.things.db.PersistentThing._

  private var snapshotTask: Option[Cancellable] = None
  private var isSnapshot = false

  override def persistenceId = self.path.toStringWithoutAddress

  def mediator = ThingExtension(context.system).topicRegion

  def isNotified = ThingExtension(context.system).Settings.enableNotification

  override def updateState(event: Any, newState: Thing.State) {
    super.updateState(event, newState)
    event match {
      case ValueRemoved | ValuePut(_) => isSnapshot = false
      case _                          =>
    }
  }

  def receiveCommand: Receive = {
    case request: Request =>
      key = request.key
      val handler = handleCommand orElse working
      context.become(handler)
      handler(request)
  }

  def handleCommand: Receive = {
    case Put(_, value)                    => persistAsync(ValuePut(value))(working)
    case Remove(_)                        => persistAsync(ValueRemoved)(working)
    case Get(_)                           => defer(ValueGot)(working)
    case AskStatus(_)                     => defer(ValueStatus)(working)
    case SaveSnapshotFailure(_, reason)   => log.error("Failed to save snapshot: {}", reason)
    case PersistenceFailure(_, _, reason) => log.error("Failed to persistence: {}", reason)
    case SaveSnapshotSuccess(metadata)    => deleteMessages(metadata.sequenceNr, permanent = true)
    case SaveSnapshot if !isSnapshot && recoveryFinished =>
      saveSnapshot(snapshot = state)
      isSnapshot = true
  }

  def receiveRecover: Receive = {
    case event: Event => working(event)
    case SnapshotOffer(metadata, offeredSnapshot: Thing.State) =>
      log.info("Recovering from offeredSnapshot: {}", offeredSnapshot)
      state = offeredSnapshot
    case x: SnapshotOffer  => log.warning("Recovering received unknown: {}", x)
    case RecoveryCompleted =>
  }

  override def preStart() = {
    enableSnapshot()
    super.preStart()
  }

  def enableSnapshot() {
    log.info("Enabled Snapshot Tick")
    snapshotTask foreach {
      _.cancel
    }
    snapshotTask = Some(context.system.scheduler.schedule(2.minutes, 2.minutes, self, SaveSnapshot))
  }

  override def unhandled(msg: Any): Unit = msg match {
    case ReceiveTimeout => doStop()
    case _              => super.unhandled(msg)
  }

  override def doStop() {
    disableSnapshot()
    context.parent ! Passivate(stopMessage = PoisonPill)
  }

  // passivated the entity when no activity
  context.setReceiveTimeout(10.minutes)

  def disableSnapshot() {
    log.info("Disabled Snapshot Tick")
    snapshotTask foreach {
      _.cancel
    }
    snapshotTask = None
  }
}
