package me.atomd.things

import akka.actor._
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.event.Logging
import akka.routing.BroadcastRoutingLogic
import akka.routing.ConsistentHashingRoutingLogic
import akka.routing.RandomRoutingLogic
import akka.routing.RoundRobinRoutingLogic
import me.atomd.things.db.LocalThingRegion
import me.atomd.things.db.PersistentThing
import me.atomd.things.db.Thing
import me.atomd.things.db.TransientThing
import me.atomd.things.mq.LocalTopicRegion
import me.atomd.things.mq.Topic

object ThingExtension extends ExtensionId[ThingExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): ThingExtension = super.get(system)

  override def lookup(): ExtensionId[_ <: Extension] = ThingExtension

  override def createExtension(system: ExtendedActorSystem): ThingExtension = new ThingExtension(system)
}

class ThingExtension(system: ExtendedActorSystem) extends Extension {

  private val log = Logging(system, "Things")
  lazy val thingProps: Props = if (Settings.enablePersistence) {
    PersistentThing.props()
  }
  else {
    TransientThing.props()
  }
  lazy val isNotified = Settings.enableNotification
  lazy val topicProps: Props = {
    Topic.props(groupRoutingLogic)
  }
  private lazy val groupRoutingLogic = {
    Settings.config.getString("routing-logic") match {
      case "random"             => RandomRoutingLogic()
      case "round-robin"        => RoundRobinRoutingLogic()
      case "consistent-hashing" => ConsistentHashingRoutingLogic(system)
      case "broadcast"          => BroadcastRoutingLogic()
      case other                => throw new IllegalArgumentException(s"Unknown 'routing-logic': [$other]")
    }
  }

  private lazy val localThingRegion = {
    val region = system.actorOf(LocalThingRegion.props(TransientThing.props()), Thing.shardName)
    ClusterReceptionistExtension(system).registerService(region)
    region
  }

  private lazy val localTopicRegion = {
    val region = system.actorOf(LocalTopicRegion.props(Topic.props(groupRoutingLogic)), Topic.shardName)
    ClusterReceptionistExtension(system).registerService(region)
    region
  }

  /**
    * Should start sharding before by: ConnectionSession.startSharding(system, Option[sessionProps])
    */
  def ThingRegion = if (Settings.isCluster) {
    Thing.shardRegion(system)
  }
  else {
    localThingRegion
  }

  /**
    * Should start sharding before by: Topic.startSharding(system, Option[topicProps])
    */
  def topicRegion = if (Settings.isCluster) {
    Topic.shardRegion(system)
  }
  else {
    localTopicRegion
  }

  def thingClient = Thing(system).clusterClient
  def topicClient = Topic(system).clusterClient

  /**
    * INTERNAL API
    */
  private[things] object Settings {
    val config = system.settings.config.getConfig("things")
    val isCluster: Boolean = config.getString("mode") == "cluster"
    val enablePersistence: Boolean = config.getBoolean("server.enable-persistence")
    val enableNotification: Boolean = config.getBoolean("server.enable-notification")
  }
}
