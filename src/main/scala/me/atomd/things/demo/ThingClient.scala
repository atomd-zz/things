package me.atomd.things.demo

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.Unsubscribe
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorSubscriber
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.WatermarkRequestStrategy
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import me.atomd.things.ThingExtension
import me.atomd.things.db._
import me.atomd.things.mq.Queue
import me.atomd.things.mq.Topic

import scala.collection.immutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._

class Receiver() extends ActorSubscriber with ActorLogging {
  override val requestStrategy = WatermarkRequestStrategy(10)

  def receive = {
    case OnNext(value) =>
      println("Observed: " + value)
  }
}

object ThingClient extends App {

  val usage =
    """
      Usage: ThingTestClusterServer [db|topic] [host] [port]
    """
  implicit val timeout = Timeout(30.seconds)
  val arglist = args.toList

  if (args.length == 0) {
    exitWithUsage
  }
  val contacts = arglist.map { address => s""""akka.tcp://ThingSystem@${address}/user/receptionist"""" }.mkString("\n")
  val extraCfg =
    s"""
          things.client.initial-contacts-points = [\n$contacts\n]
          akka.remote.netty.tcp.port=0
        """
  val config = ConfigFactory.parseString(extraCfg).withFallback(ConfigFactory.load("client"))
  val system = thingSystem(config)
  val thingExt = ThingExtension(system)
  var subscribeTopics = HashMap[String, ActorRef]()

  def exitWithUsage = {
    println(usage)
    sys.exit(1)
  }

  def thingSystem(config: Config) = {
    ActorSystem("ThingSystem", config)
  }
  Await.result(thingExt.thingClient ? AskStatus("0"), timeout.duration).asInstanceOf[Status]
  subscribe(Topic.EMPTY)

  def subscribe(topic: String) = {
    val queue = system.actorOf(Queue.props())
    val receiver = system.actorOf(Props(new Receiver()))
    ActorPublisher(queue).subscribe(ActorSubscriber(receiver))
    val result = Await.result(thingExt.topicClient ? Subscribe(topic, queue), timeout.duration)
    subscribeTopics += (topic -> queue)
  }

  def unsubscribe(topic: String) = {
    val queueOption = subscribeTopics.get(topic)
    if (queueOption.isDefined) {
      val queue = queueOption.get
      val result = Await.result(thingExt.topicClient ? Unsubscribe(topic, queue), timeout.duration)
      subscribeTopics -= topic
    }
  }

  for (ln <- io.Source.stdin.getLines) {
    ln.split(" ").toList match {
      case "sub" :: "things" :: tail =>
        subscribe(Topic.GLOBAL)
        println("Result: Success")

      case "sub" :: topic :: tail =>
        subscribe(topic)
        println("Result: Success")

      case "unsub" :: topic :: tail =>
        unsubscribe(topic)
        println("Result: Success")

      case "pub" :: topic :: message :: tail =>
        thingExt.topicClient ! Publish(topic, message)
        println("Result: Success")

      case "put" :: "range" :: start :: end :: tail =>
        var m: Map[String, Any] = Map()
        for (i <- start.toInt to end.toInt) {
          val time = System.currentTimeMillis
          m += (i.toString -> time)
          Await.result(thingExt.thingClient ? Put(i.toString, System.currentTimeMillis), timeout.duration)
        }
        println("Result: " + m)

      case "put" :: key :: value :: tail =>
        val result = Await.result(thingExt.thingClient ? Put(key, value), timeout.duration)
        println("Result: " + result + "\n")

      case "get" :: "range" :: start :: end :: tail =>
        var m: Map[String, Any] = Map()
        for (i <- start.toInt to end.toInt) {
          val result = Await.result(thingExt.thingClient ? Get(i.toString), timeout.duration).asInstanceOf[Result]
          m += (i.toString -> result.value)
        }
        println("Result: " + m)

      case "get" :: key :: tail =>
        val result = Await.result(thingExt.thingClient ? Get(key), timeout.duration).asInstanceOf[Result]
        println("Value is: " + result.value + "\n")

      case "remove" :: key :: tail =>
        val result = Await.result(thingExt.thingClient ? Remove(key), timeout.duration)
        println("Result: " + result + "\n")

      case "status" :: key :: tail =>
        val result = Await.result(thingExt.thingClient ? AskStatus(key), timeout.duration).asInstanceOf[Status]
        println("Result: " + result + "\n")

      case head :: tail if head != "" =>
        println("Input errors, please try again.")

      case _ =>
    }
  }
}