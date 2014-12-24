package me.atomd.things

import java.io.File
import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.contrib.pattern.ClusterClient
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.SubscribeAck
import akka.pattern.ask
import akka.persistence.Persistence
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.persistence.journal.leveldb.SharedLeveldbStore
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorSubscriber
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.WatermarkRequestStrategy
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import me.atomd.things.ThingClusterSpec.TopicAggregatorReceiver
import me.atomd.things.db.AskStatus
import me.atomd.things.db.Status
import me.atomd.things.db.Thing
import me.atomd.things.mq.Aggregator
import me.atomd.things.mq.Queue
import me.atomd.things.mq.Topic
import org.iq80.leveldb.util.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._

object ThingClusterSpecConfig extends MultiNodeConfig {

  val controller = role("controller")
  val topic1 = role("topic1")
  val topic2 = role("topic2")
  val db1 = role("db1")
  val db2 = role("db2")
  val client1 = role("client1")
  val client2 = role("client2")

  val host = "127.0.0.1"

  val port1 = 8081
  val port2 = 8082

  commonConfig(ConfigFactory.parseString(
    """
      akka.loglevel = INFO
      akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
      akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
      akka.persistence.journal.leveldb-shared.store {
        native = off
        dir = "target/test-shared-journal"
      }
      akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
      things.mode = "cluster"
      akka.cluster.seed-nodes = [
        "akka.tcp://ThingClusterSpec@localhost:2601",
        "akka.tcp://ThingClusterSpec@localhost:2701"
      ]
    """))

  nodeConfig(topic1) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2601
        akka.contrib.cluster.sharding.role = "topic"
        akka.cluster.roles = ["topic", "db"]
      """)
  }

  nodeConfig(topic2) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2602
        akka.contrib.cluster.sharding.role = "topic"
        akka.cluster.roles = ["topic"]
      """)
  }

  nodeConfig(db1) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2701
        akka.contrib.cluster.sharding.role = "db"
        akka.cluster.roles = ["db", "topic"]
      """)
  }

  nodeConfig(db2) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2702
        akka.contrib.cluster.sharding.role = "db"
        akka.cluster.roles = ["db", "topic"]
      """)
  }

  // We set topic and session node as the candicate of first starting node only,
  // so transport is not necessary to contain role "session" or "topic"
  nodeConfig(client1, client2) {
    ConfigFactory.parseString(
      """
        akka.cluster.roles =["client"]
        things {
            client.initial-contacts-points = [
                "akka.tcp://ThingClusterSpec@localhost:2601/user/receptionist",
                "akka.tcp://ThingClusterSpec@localhost:2701/user/receptionist"
            ]
        }
      """)
  }
}

class ThingClusterSpecMultiJvmNode1 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode2 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode3 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode4 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode5 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode6 extends ThingClusterSpec
class ThingClusterSpecMultiJvmNode7 extends ThingClusterSpec

object ThingClusterSpec {

  class Service extends Actor with ActorLogging {

    import context.dispatcher
    def receive = running

    val running: Receive = {
      case "Hello" =>
        sender() ! "World"
    }
  }

  object ThingClient {
    private case object Tick
  }

  class ThingClient extends Actor with ActorLogging {

    import context.dispatcher
    import me.atomd.things.ThingClusterSpec.ThingClient._
    import me.atomd.things.db._

    val tickTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Tick)
    val thingRegion = ClusterSharding(context.system).shardRegion(Thing.shardName)
    val from = Cluster(context.system).selfAddress.hostPort

    override def postStop(): Unit = {
      super.postStop()
      tickTask.cancel()
    }

    def receive = running

    val running: Receive = {
      case Tick =>
        val key = UUID.randomUUID().toString
        val value = UUID.randomUUID().toString
        implicit val timeout = Timeout(5.seconds)
        thingRegion ! Put(key, value)
        val future = thingRegion ? Get(key)
      // val result = Await.result(future, timeout.duration).asInstanceOf[result]
    }
  }

  class Receiver(probe: ActorRef) extends ActorSubscriber with ActorLogging {
    override val requestStrategy = WatermarkRequestStrategy(10)
    def receive = {
      case OnNext(value) =>
        println("observed: " + value)
    }
  }

  class TopicAggregatorReceiver(probe: ActorRef) extends ActorSubscriber with ActorLogging {
    override val requestStrategy = WatermarkRequestStrategy(10)
    def receive = {
      case OnNext(value: Aggregator.Available) =>
        log.info("Got {}", value)
        probe ! value
      case OnNext(value: Aggregator.Unreachable) =>
        log.info("Got {}", value)
        probe ! value
      case OnNext(value) =>
        println("observed: " + value)
    }
  }

}

class ThingClusterSpec extends MultiNodeSpec(ThingClusterSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import ThingClusterSpecConfig._
  import ThingClusterSpec._

  def initialParticipants = roles.size

  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir"
  ).map(s => new File(system.settings.config.getString(s)))

  override protected def atStartup() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteRecursively(dir))
    }
  }

  override protected def afterTermination() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteRecursively(dir))
    }
  }

  "Sharded Things Cluster" must {

    "setup shared journal" in {
      // start the Persistence extension
      Persistence(system)
      runOn(controller) {
        ClusterMonitor.startMonitor(system)
        system.actorOf(Props[SharedLeveldbStore], "store")
      }
      enterBarrier("peristence-started")

      runOn(topic1, topic2, db1, db2) {
        system.actorSelection(node(controller) / "user" / "store") ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }
      enterBarrier("setup-persistence")
    }

    "start cluster" in within(30.seconds) {
      val cluster = Cluster(system)

      runOn(db1) { cluster join node(controller).address }
      runOn(db2) { cluster join node(controller).address }
      runOn(topic1) { cluster join node(controller).address }
      runOn(topic2) { cluster join node(controller).address }
      runOn(client1) { cluster join node(controller).address }
      runOn(client2) { cluster join node(controller).address }

      // with roles: db, topic
      runOn(db1, db2) {
        // if it starts as the first node, should also start topicAggregator's single manager
        Topic.startTopicAggregator(system, role = Some("topic"))

        Topic.startSharding(system, None)
        Thing.startSharding(system, Some(ThingExtension(system).thingProps))
      }

      // with roles: topic, session
      runOn(topic1, topic2) {
        // should start the proxy too, since topics should report to topicAggregator via this proxy
        Topic.startTopicAggregator(system, role = Some("topic"))
        Topic.startTopicAggregatorProxy(system, role = Some("topic"))

        Topic.startSharding(system, Some(ThingExtension(system).topicProps))
        // if it starts as the first node, should also start ConnectionSession's coordinate
        Thing.startSharding(system, None)
      }

      // with roles: client
      runOn(client1, client2) {
        // pass
      }

      runOn(topic1, topic2, db1, db2, client1, client2) {
        awaitAssert {
          self ! cluster.state.members.filter(_.status == MemberStatus.Up).size
          expectMsg(7)
        }
        enterBarrier("start-cluster")
      }

      runOn(controller) {
        enterBarrier("start-cluster")
      }
    }

    "verify cluster sevices" in within(30.seconds) {
      runOn(topic1, topic2) {
        def topicAggregatorProxy = Topic(system).topicAggregatorProxy
        val queue = system.actorOf(Queue.props())
        topicAggregatorProxy ! Subscribe(Topic.EMPTY, queue)
        expectMsgType[SubscribeAck]

        val serviceA = system.actorOf(Props[Service], "serviceA")
        ClusterReceptionistExtension(system).registerService(serviceA)
      }

      runOn(db1, db2) {
        def thingRegion = Thing.shardRegion(system)
        log.info("thingRegion: {}", thingRegion)
        thingRegion ! AskStatus("0")
        expectMsgType[Status]

        val serviceA = system.actorOf(Props[Service], "serviceA")
        ClusterReceptionistExtension(system).registerService(serviceA)
      }
      enterBarrier("verified-cluster-services")
    }

    "start client sevices" in within(60.seconds) {
      runOn(client1) {

        val thingExt = ThingExtension(system)
        val topicAggregatorClient = Topic(system).topicAggregatorClient

        val topicsQueue = system.actorOf(Queue.props())
        val topicsReceiver = system.actorOf(Props(new TopicAggregatorReceiver(self)))
        ActorPublisher(topicsQueue).subscribe(ActorSubscriber(topicsReceiver))
        topicAggregatorClient ! Subscribe(Topic.EMPTY, topicsQueue)
        expectMsgType[SubscribeAck]

        val queue = system.actorOf(Queue.props())
        val receiver = system.actorOf(Props(new Receiver(self)))
        ActorPublisher(queue).subscribe(ActorSubscriber(receiver))
        thingExt.topicClient ! Subscribe(Topic.EMPTY, Some("group1"), queue)
        expectMsgAllClassOf(classOf[Aggregator.Available], classOf[SubscribeAck])

        topicAggregatorClient ! Aggregator.AskStats
        expectMsgPF(5.seconds) {
          case Aggregator.Stats(xs) if xs.values.toList.contains(Topic.EMPTY) =>
            log.info("aggregator topics: {}", xs); assert(true)
          case x => log.error("Wrong aggregator topics: {}", x); assert(false)
        }
      }

      runOn(client2) {
        val thingExt = ThingExtension(system)
        val queue = system.actorOf(Queue.props())
        val receiver = system.actorOf(Props(new Receiver(self)))
        ActorPublisher(queue).subscribe(ActorSubscriber(receiver))

        thingExt.topicClient ! Subscribe(Topic.EMPTY, Some("group2"), queue)
        expectMsgType[SubscribeAck]

        //queueOfBusiness3 = queue
      }
      enterBarrier("started-client-business")
    }
  }
}
