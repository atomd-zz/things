package me.atomd.things

import java.io.File

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.SubscribeAck
import akka.contrib.pattern.DistributedPubSubMediator.Unsubscribe
import akka.contrib.pattern.DistributedPubSubMediator.UnsubscribeAck
import akka.contrib.pattern.DistributedPubSubMediator.Publish
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
import me.atomd.things.db._
import me.atomd.things.mq._
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
  val client3 = role("client3")

  commonConfig(ConfigFactory.parseString(
    """
      things.mode = "cluster"
      akka.loglevel = INFO
      akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
      akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
      akka.persistence.journal.leveldb-shared.store {
        native = off
        dir = "target/test-shared-journal"
      }
    """))

  nodeConfig(controller) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2500
        akka.cluster.roles = ["controller", ]
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingClusterSpec@localhost:2500",
        ]
      """)
  }

  nodeConfig(topic1) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2601
        akka.contrib.cluster.sharding.role = "topic"
        akka.cluster.roles = ["topic", "db"]
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingClusterSpec@localhost:2500",
        ]
      """)
  }

  nodeConfig(topic2) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2602
        akka.contrib.cluster.sharding.role = "topic"
        akka.cluster.roles = ["topic", "db"]
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingClusterSpec@localhost:2500",
        ]
      """)
  }

  nodeConfig(db1) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2701
        akka.contrib.cluster.sharding.role = "db"
        akka.cluster.roles = ["db", "topic"]
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingClusterSpec@localhost:2500",
        ]
      """)
  }

  nodeConfig(db2) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 2702
        akka.contrib.cluster.sharding.role = "db"
        akka.cluster.roles = ["db", "topic"]
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingClusterSpec@localhost:2500",
        ]
      """)
  }

  nodeConfig(client1, client2, client3) {
    ConfigFactory.parseString(
      """
        akka.remote.netty.tcp.port = 0
        things.client.initial-contacts-points = [
            "akka.tcp://ThingClusterSpec@localhost:2601/user/receptionist",
            "akka.tcp://ThingClusterSpec@localhost:2602/user/receptionist",
        ]
        akka.actor.provider = "akka.remote.RemoteActorRefProvider"
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
class ThingClusterSpecMultiJvmNode8 extends ThingClusterSpec


object ThingClusterSpec {

  class Receiver(probe: ActorRef) extends ActorSubscriber with ActorLogging {
    override val requestStrategy = WatermarkRequestStrategy(10)
    def receive = {
      case OnNext(value) =>
        log.info("Observed: " + value)
        probe ! value
    }
  }

  class TopicAggregatorReceiver(probe: ActorRef) extends ActorSubscriber with ActorLogging {
    override val requestStrategy = WatermarkRequestStrategy(10)
    def receive = {
      case OnNext(value: Aggregator.Available) =>
        log.info("TopicAggregatorReceiver Got {}", value)
        probe ! value
      case OnNext(value: Aggregator.Unreachable) =>
        log.info("TopicAggregatorReceiver Got {}", value)
        probe ! value
      case OnNext(value) =>
        log.info("TopicAggregatorReceiver Get: " + value)
    }
  }
}

class ThingClusterSpec extends MultiNodeSpec(ThingClusterSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import me.atomd.things.ThingClusterSpec._
  import me.atomd.things.ThingClusterSpecConfig._

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

    "Setup Shared Journal" in {
      runOn(controller) {
        ClusterMonitor.startMonitor(system)
        system.actorOf(Props[SharedLeveldbStore], "store")
        enterBarrier("peristence-started")
      }

      runOn(topic1, topic2, db1, db2) {
        enterBarrier("peristence-started")
        Persistence(system)
        system.actorSelection(node(controller) / "user" / "store") ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }

      runOn(client1, client2, client3) {
        enterBarrier("peristence-started")
      }
      enterBarrier("setup-persistence")
    }

    "Start Cluster" in within(30.seconds) {

      runOn(topic1, topic2) {
        Topic.startTopicAggregator(system, role = Some("topic"))
        Topic.startTopicAggregatorProxy(system, role = Some("topic"))

        Topic.startSharding(system, Some(ThingExtension(system).topicProps))
        Thing.startSharding(system, None)
      }

      runOn(db1, db2) {
        Topic.startTopicAggregator(system, role = Some("topic"))

        Topic.startSharding(system, None)
        Thing.startSharding(system, Some(ThingExtension(system).thingProps))
      }

      runOn(controller, topic1, topic2, db1, db2) {
        val cluster = Cluster(system)
        awaitAssert {
          self ! cluster.state.members.filter(_.status == MemberStatus.Up).size
          expectMsg(5)
        }
        enterBarrier("start-cluster")
      }

      runOn(client1, client2, client3) {
        enterBarrier("start-cluster")
      }
    }

    "Verify Cluster Sevices" in within(30.seconds) {
      runOn(topic1, topic2) {
        val topicAggregatorProxy = Topic(system).topicAggregatorProxy
        val queue = system.actorOf(Queue.props())
        topicAggregatorProxy ! Subscribe(Topic.EMPTY, queue)
        expectMsgType[SubscribeAck]
      }

      runOn(db1, db2) {
        val thingRegion = Thing.shardRegion(system)
        thingRegion ! AskStatus("0")
        expectMsgType[Status]
      }
      enterBarrier("verified-cluster-services")
    }

    "Test Client Sevices" in within(60.seconds) {
      runOn(client1) {
        val thingExt = ThingExtension(system)
        val topicAggregatorClient = Topic(system).topicAggregatorClient

        // start topic aggregator Receiver
        val topicsQueue = system.actorOf(Queue.props())
        val topicsReceiver = system.actorOf(Props(new TopicAggregatorReceiver(self)))
        ActorPublisher(topicsQueue).subscribe(ActorSubscriber(topicsReceiver))
        topicAggregatorClient ! Subscribe(Topic.EMPTY, topicsQueue)
        expectMsgType[SubscribeAck]
        enterBarrier("started-topic-aggregator")

        // start subscribe Topic.GLOBAL topic
        val queue1 = system.actorOf(Queue.props())
        val receiver = system.actorOf(Props(new Receiver(self)))
        ActorPublisher(queue1).subscribe(ActorSubscriber(receiver))
        thingExt.topicClient ! Subscribe(Topic.GLOBAL, queue1)
        expectMsgAllClassOf(classOf[Aggregator.Available], classOf[SubscribeAck])

        // check Aggregator Status
        topicAggregatorClient ! Aggregator.AskStats
        expectMsgPF(5.seconds) {
          case Aggregator.Stats(xs) if xs.values.toList.contains(Topic.GLOBAL) =>
            log.info("aggregator topics: {}", xs); assert(true)
          case x =>
            log.error("Wrong aggregator topics: {}", x); assert(false)
        }
        enterBarrier("subscribed-global-topic")

        expectMsg(Created("key1", "value1"))
        expectMsg(Modified("key1", "value2"))
        expectMsg(Removed("key1"))
        thingExt.topicClient ! Unsubscribe(Topic.GLOBAL, queue1)
        expectMsgType[UnsubscribeAck]
        enterBarrier("checked-basic-db-functions")

        val queue2 = system.actorOf(Queue.props())
        ActorPublisher(queue2).subscribe(ActorSubscriber(system.actorOf(Props(new Receiver(self)))))
        thingExt.topicClient ! Subscribe("topic-func", queue2)
        expectMsgAllClassOf(classOf[Aggregator.Available], classOf[SubscribeAck])
        enterBarrier("prepared-basic-topic-functions")

        expectMsg("msg1")
        expectMsg("msg2")
        expectMsg("msg3")
        expectMsg("msg4")
        expectMsg("msg5")
        thingExt.topicClient ! Unsubscribe("topic-func", queue2)
        expectMsgType[UnsubscribeAck]
        enterBarrier("checked-basic-topic-functions")
      }

      runOn(client2) {
        val thingExt = ThingExtension(system)

        enterBarrier("started-topic-aggregator")
        enterBarrier("subscribed-global-topic")

        // create value
        thingExt.thingClient ! Put("key1", "value1")
        expectMsg(Success)
        thingExt.thingClient ! Get("key1")
        expectMsg(Result("value1"))
        // modify value
        thingExt.thingClient ! Put("key1", "value2")
        expectMsg(Success)
        thingExt.thingClient ! Get("key1")
        expectMsg(Result("value2"))
        // remove value
        thingExt.thingClient ! Remove("key1")
        expectMsg(Success)
        thingExt.thingClient ! Get("key1")
        expectMsg(Result(None))
        enterBarrier("checked-basic-db-functions")
        enterBarrier("prepared-basic-topic-functions")

        thingExt.topicClient ! Publish("topic-func", "msg1")
        thingExt.topicClient ! Publish("topic-func", "msg2")
        thingExt.topicClient ! Publish("topic-func", "msg3")
        thingExt.topicClient ! Publish("topic-func", "msg4")
        thingExt.topicClient ! Publish("topic-func", "msg5")
        enterBarrier("checked-basic-topic-functions")
      }

      runOn(controller, topic1, topic2, db1, db2, client3) {
        enterBarrier("started-topic-aggregator")
        enterBarrier("subscribed-global-topic")
        enterBarrier("checked-basic-db-functions")
        enterBarrier("prepared-basic-topic-functions")
        enterBarrier("checked-basic-topic-functions")
      }
      enterBarrier("verified-client-services")
    }
  }
}
