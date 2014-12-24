package me.atomd.things.db

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.contrib.pattern.ClusterClient
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.contrib.pattern.ShardRegion
import akka.event.LoggingAdapter
import me.atomd.things.RemoteAddressExtension
import me.atomd.things.ThingExtension
import me.atomd.things.mq.Topic

object Thing {

  def props(): Props = Props(classOf[Thing])

  val shardName: String = "Things"

  val idExtractor: ShardRegion.IdExtractor = {
    case request: Request => (request.key, request)
  }

  val shardResolver: ShardRegion.ShardResolver = {
    case request: Request => (math.abs(request.key.hashCode) % 100).toString
  }

  def shardRegion(system: ActorSystem) = ClusterSharding(system).shardRegion(shardName)

  def shardRegionPath(system: ActorSystem) = {
    if (ThingExtension(system).Settings.isCluster) {
      val shardingGuardianName = system.settings.config.getString("akka.contrib.cluster.sharding.guardian-name")
      s"/user/$shardingGuardianName/$shardName"
    } else {
      s"/user/$shardName"
    }
  }

  def startSharding(system: ActorSystem, entryProps: Option[Props]) {
    val sharding = ClusterSharding(system)
    sharding.start(
      entryProps = entryProps,
      typeName = shardName,
      idExtractor = idExtractor,
      shardResolver = shardResolver)
    if (entryProps.isDefined) ClusterReceptionistExtension(system).registerService(sharding.shardRegion(shardName))
  }

  class ClusterClientBroker(servicePath: String, originalClient: ActorRef) extends Actor with ActorLogging {
    def receive: Actor.Receive = {
      case cmd: Command => originalClient forward ClusterClient.Send(servicePath, cmd, localAffinity = false)
    }
  }

  final class SystemSingletons(system: ActorSystem) {
    lazy val originalClusterClient = {
      import scala.collection.JavaConversions._
      val initialContacts = system.settings.config.getStringList("things.client.initial-contacts-points").toSet
      system.actorOf(ClusterClient.props(initialContacts map system.actorSelection), "things-db-cluster-client")
    }

    lazy val clusterClient = {
      val path = shardRegionPath(system)
      system.actorOf(Props(classOf[ClusterClientBroker], path, originalClusterClient))
    }
  }

  private var singletons: SystemSingletons = _
  private val singletonsMutex = new AnyRef()

  def apply(system: ActorSystem): SystemSingletons = {
    if (singletons eq null) {
      singletonsMutex synchronized {
        if (singletons eq null) {
          singletons = new SystemSingletons(system)
        }
      }
    }
    singletons
  }

  object State {
    val empty = State(None)
  }

  case class State(value: Any) {
    def updated(event: Event): State = event match {
      case ValuePut(value) => State(value)
      case ValueRemoved    => State.empty
      case _               => this
    }
  }

}

trait Thing { _: Actor =>

  import me.atomd.things.db.Thing._

  def log: LoggingAdapter
  def isNotified: Boolean
  def mediator: ActorRef
  def handleCommand: Receive

  var state = State.empty
  var key: String = null

  private val startTime = System.currentTimeMillis

  def updateState(evt: Any, newState: State) {
    state = newState
  }

  def working: Receive = {
    case ValueGot =>
      sender() ! Result(state.value)
      log.info("Value Got: value: {}", state.value)
    case event @ ValuePut(value) =>
      if (isNotified) {
        val message = state match {
          case State.empty => Created(key, value)
          case _           => Modified(key, value)
        }
        mediator ! Publish(Topic.GLOBAL, message)
      }
      state = state.updated(event)
      sender() ! Success
      log.info("Value saved: {}", state.value)
    case event @ ValueRemoved =>
      state = state.updated(event)
      sender() ! Success
      if (isNotified) {
        mediator ! Publish(Topic.GLOBAL, Removed(key))
      }
      log.info("Value removed: {}", state.value)
    case ValueStatus =>
      val remoteAddr = RemoteAddressExtension(context.system).address
      val remotePath = self.path.toStringWithAddress(remoteAddr)
      sender() ! Status(key, System.currentTimeMillis - startTime, remotePath)
  }

  def doStop() {
    self ! PoisonPill
  }
}
