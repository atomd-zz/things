package me.atomd.things.demo

import akka.actor._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import me.atomd.things.ThingExtension
import me.atomd.things.mq.Topic

object LocalServer extends App {

  val usage =
    """
      Usage: LocalServer [db|topic] [port]
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

  val port = args(0)
  val host = "127.0.0.1"
  var extraCfg =
    s"""
        things.mode = "local"
        akka.remote.netty.tcp.hostname="$host"
        akka.remote.netty.tcp.port=$port
        akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
        akka.cluster.roles = ["db", "topic"]
        akka.cluster.seed-nodes = [
          "akka.tcp://ThingSystem@$host:$port",
        ]
    """.stripMargin

  val config = ConfigFactory.parseString(extraCfg).withFallback(ConfigFactory.load("local"))
  val system = thingSystem(config)
  val thingExt = ThingExtension(system)
  Topic.startTopicAggregator(system, Some("topic"))
  Topic.startTopicAggregatorProxy(system, Some("topic"))
  thingExt.ThingRegion
  thingExt.topicRegion
}
