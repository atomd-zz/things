package me.atomd.things.demo

import akka.actor._
import akka.persistence.Persistence
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import me.atomd.things.ClusterMonitor
import me.atomd.things.ThingExtension
import me.atomd.things.db._
import me.atomd.things.mq.Topic

object ExpandedAddress {
  def unapplySeq(address: String): Option[Seq[String]] = {
    val parts = address.split(":")
    if (parts.length == 2)
      Some(parts)
    else
      None
  }
}

object ClusterServer extends App {

  val usage =
    """
      Usage: ClusterServer [db|topic] [host] [port]
    """

  def exitWithUsage = {
    println(usage)
    sys.exit(1)
  }

  def thingSystem(config: Config) = {
    ActorSystem("ThingSystem", config)
  }

  if (args.length == 0) {
    exitWithUsage
  }

  args.toList match {
    case "monitor" :: tail =>
      var extraCfg =
        s"""
        |akka.cluster.roles = ["db", "topic", "monitor"]
        |akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        |""".stripMargin
      if (tail != Nil) {
        val contacts = tail.map { address => s""""akka.tcp://ThingSystem@${address}"""" }.mkString("\n")
        extraCfg += s"cluster.seed-nodes = [\n$contacts\n]".stripMargin
      }
      val config = ConfigFactory.parseString(extraCfg).withFallback(ConfigFactory.load("cluster"))
      val system = thingSystem(config)
      ClusterMonitor.startMonitor(system)

    case "db" :: address :: tail =>
      val ExpandedAddress(host, port) = address
      var extraCfg =
        s"""
          akka.remote.netty.tcp.hostname="$host"
          akka.remote.netty.tcp.port=$port
          akka.contrib.cluster.sharding.role = "db"
          akka.cluster.roles = ["db", "topic"]
          akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        """.stripMargin
      if (tail != Nil) {
        val contacts = tail.map { address => s""""akka.tcp://ThingSystem@${address}"""" }.mkString("\n")
        extraCfg += s"cluster.seed-nodes = [\n$contacts\n]".stripMargin
      }
      val config = ConfigFactory.parseString(extraCfg).withFallback(ConfigFactory.load("cluster"))
      val system = thingSystem(config)
      Persistence(system)
      // if it starts as the first node, should also start topicAggregator's single manager
      Topic.startTopicAggregator(system, role = Some("topic"))
      Topic.startSharding(system, None)
      Thing.startSharding(system, Some(ThingExtension(system).thingProps))

    case "topic" :: address :: tail =>
      val ExpandedAddress(host, port) = address
      var extraCfg =
        s"""
          akka.remote.netty.tcp.hostname="$host"
          akka.remote.netty.tcp.port=$port
          akka.contrib.cluster.sharding.role = "db"
          akka.cluster.roles = ["db", "topic"]
          akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        """.stripMargin
      if (tail != Nil) {
        val contacts = tail.map { address => s""""akka.tcp://ThingSystem@${address}"""" }.mkString("\n")
        extraCfg += s"cluster.seed-nodes = [\n$contacts\n]".stripMargin
      }
      val config = ConfigFactory.parseString(extraCfg).withFallback(ConfigFactory.load("cluster"))
      val system = thingSystem(config)
      Persistence(system)
      Topic.startTopicAggregator(system, Some("topic"))
      // should start the proxy too, since topics should report to topicAggregator via this proxy
      Topic.startTopicAggregatorProxy(system, Some("topic"))
      Topic.startSharding(system, Some(ThingExtension(system).topicProps))
      // if it starts as the first node, should also start ConnectionSession's coordinate
      Thing.startSharding(system, None)

    case _ =>
      exitWithUsage
  }
}
