package me.atomd.things.mq

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.contrib.pattern.ClusterClient
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ClusterSingletonProxy
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.SubscribeAck
import akka.contrib.pattern.DistributedPubSubMediator.Unsubscribe
import akka.contrib.pattern.DistributedPubSubMediator.UnsubscribeAck
import akka.contrib.pattern.ShardRegion
import akka.routing.Router
import akka.routing.RoutingLogic
import me.atomd.things.ThingExtension
import me.atomd.things.mq.Aggregator.ReportingData

object Topic {

  val EMPTY = "TOPIC-EMPTY"
  val GLOBAL = "TOPIC-GLOBAL"
  val shardName: String = "Topics"

  val idExtractor: ShardRegion.IdExtractor = {
    case x: Subscribe => (x.topic, x)
    case x: Unsubscribe => (x.topic, x)
    case x: SubscribeAck => (x.subscribe.topic, x)
    case x: UnsubscribeAck => (x.unsubscribe.topic, x)
    case x: Publish => (x.topic, x)
  }
  val shardResolver: ShardRegion.ShardResolver = {
    case x: Subscribe => hashForShard(x.topic)
    case x: Unsubscribe => hashForShard(x.topic)
    case x: SubscribeAck => hashForShard(x.subscribe.topic)
    case x: UnsubscribeAck => hashForShard(x.unsubscribe.topic)
    case x: Publish => hashForShard(x.topic)
  }
  val TopicAggregator = "aggregator-topic"
  val TopicAggregatorPath = "/user/" + Aggregator.singletonManagerNameForAggregate(TopicAggregator) + "/" + TopicAggregator
  val TopicAggregatorProxyName = "topicAggregatorProxy"
  val TopicAggregatorProxyPath = "/user/" + TopicAggregatorProxyName
  private val singletonsMutex = new AnyRef()
  private var singletons: SystemSingletons = _

  def props(groupRoutingLogic: RoutingLogic) = Props(classOf[Topic], groupRoutingLogic)

  def startSharding(system: ActorSystem, entryProps: Option[Props]) {
    val sharding = ClusterSharding(system)
    sharding.start(
      entryProps = entryProps,
      typeName = shardName,
      idExtractor = idExtractor,
      shardResolver = shardResolver)
    if (entryProps.isDefined) ClusterReceptionistExtension(system).registerService(sharding.shardRegion(shardName))
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

  def startTopicAggregator(system: ActorSystem, role: Option[String]) {
    Aggregator.startAggregator(system, TopicAggregator, role = role)
  }

  def startTopicAggregatorProxy(system: ActorSystem, role: Option[String]) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(singletonPath = Topic.TopicAggregatorPath, role = role),
      name = Topic.TopicAggregatorProxyName
    )
    ClusterReceptionistExtension(system).registerService(proxy)
  }

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

  private def hashForShard(topic: String) = (math.abs(topic.hashCode) % 100).toString

  final class SystemSingletons(system: ActorSystem) {
    lazy val originalClusterClient = {
      import scala.collection.JavaConversions._
      val initialContacts = system.settings.config.getStringList("things.client.initial-contacts-points").toSet
      system.actorOf(ClusterClient.props(initialContacts map system.actorSelection), "things-topic-cluster-client")
    }

    lazy val clusterClient = {
      val path = shardRegionPath(system)
      system.actorOf(Props(classOf[ClusterClientBroker], path, originalClusterClient))
    }

    lazy val topicAggregatorProxy = system.actorSelection(TopicAggregatorProxyPath)

    lazy val topicAggregatorClient = {
      system.actorOf(Props(classOf[ClusterClientBroker], TopicAggregatorProxyPath, originalClusterClient))
    }
  }

  class ClusterClientBroker(servicePath: String, originalClient: ActorRef) extends Actor with ActorLogging {
    def receive = {
      case x => originalClient forward ClusterClient.Send(servicePath, x, localAffinity = false)
    }
  }

  private case object ReportingTick

}

class Topic(groupRoutingLogic: RoutingLogic) extends Publishable with Actor with ActorLogging {

  import context.dispatcher
  import me.atomd.things.mq.Topic._

  val groupRouter = Router(groupRoutingLogic)
  val reportingTask = if (isAggregator) {
    None
  }
  else {
    topicAggregator ! ReportingData(topic)
    val settings = new Aggregator.Settings(context.system)
    Some(context.system.scheduler.schedule(settings.AggregatorReportingInterval, settings.AggregatorReportingInterval, self, ReportingTick))
  }

  def isAggregator = false

  override def postStop(): Unit = {
    super.postStop()
    reportingTask foreach {
      _.cancel()
    }
  }

  def receive: Receive = publishableBehavior orElse reportingTickBehavior

  def reportingTickBehavior: Receive = {
    case ReportingTick => topicAggregator ! ReportingData(topic)
  }

  private def topicAggregator = Topic(context.system).topicAggregatorProxy
}
