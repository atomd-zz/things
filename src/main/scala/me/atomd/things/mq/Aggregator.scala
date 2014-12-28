package me.atomd.things.mq

import akka.ConfigurationException
import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.contrib.pattern.ClusterSingletonManager
import akka.event.EventStream
import akka.remote.DefaultFailureDetectorRegistry
import akka.remote.FailureDetector
import akka.remote.FailureDetectorRegistry
import akka.routing.BroadcastRoutingLogic
import akka.routing.ConsistentHashingRoutingLogic
import akka.routing.RandomRoutingLogic
import akka.routing.RoundRobinRoutingLogic
import akka.routing.RoutingLogic
import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object Aggregator {

  def props(
    groupRoutingLogic: RoutingLogic,
    failureDetector: FailureDetectorRegistry[Address],
    unreachableReaperInterval: FiniteDuration): Props =
    Props(classOf[Aggregator], groupRoutingLogic, failureDetector, unreachableReaperInterval)

  case object AskStats
  final case class ReportingData(data: Any)
  final case class Available(address: Address, report: Any)
  final case class Unreachable(address: Address, report: Any)
  final case class Stats(reportingData: Map[Address, Any])

  // sent to self only
  private case object ReapUnreachableTick

  class Settings(system: ActorSystem) {
    val config = system.settings.config.getConfig("things")

    import config._
    import me.atomd.things.mq.Helpers._

    val Dispatcher: String = getString("akka.remote.use-dispatcher")

    def configureDispatcher(props: Props): Props = if (Dispatcher.isEmpty) props else props.withDispatcher(Dispatcher)

    val FailureDetectorConfig: Config = config.getConfig("aggregator-failure-detector")
    val AggregatorReportingInterval = FailureDetectorConfig.getMillisDuration("heartbeat-interval")
    val AggregatorFailureDetectorImplementationClass: String = FailureDetectorConfig.getString("implementation-class")
    val AggregatorUnreachableReaperInterval: FiniteDuration = {
      FailureDetectorConfig.getMillisDuration("unreachable-nodes-reaper-interval")
    } requiring (_ > Duration.Zero, "aggregator-failure-detector.unreachable-nodes-reaper-interval must be > 0")

    val groupRoutingLogic = {
      config.getString("routing-logic") match {
        case "random"             => RandomRoutingLogic()
        case "round-robin"        => RoundRobinRoutingLogic()
        case "consistent-hashing" => ConsistentHashingRoutingLogic(system)
        case "broadcast"          => BroadcastRoutingLogic()
        case other                => throw new IllegalArgumentException(s"Unknown 'routing-logic': [$other]")
      }
    }
  }

  private def createAggreratorFailureDetector(system: ActorSystem): FailureDetectorRegistry[Address] = {
    val settings = new Settings(system)
      def createFailureDetector(): FailureDetector =
        FailureDetectorLoader.load(settings.AggregatorFailureDetectorImplementationClass, settings.FailureDetectorConfig, system)

    new DefaultFailureDetectorRegistry(() => createFailureDetector())
  }

  def singletonManagerNameForAggregate(topic: String) = "aggregatorSingleton-" + topic

  def startAggregator(system: ActorSystem, aggregateTopic: String, role: Option[String]): Unit = {
    val settings = new Settings(system)
    val failureDetector = createAggreratorFailureDetector(system)

    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(settings.groupRoutingLogic, failureDetector, settings.AggregatorUnreachableReaperInterval),
        singletonName = aggregateTopic,
        terminationMessage = PoisonPill,
        role = role),
      name = singletonManagerNameForAggregate(aggregateTopic)
    )
  }
}

class Aggregator(
    groupRoutingLogic: RoutingLogic,
    failureDetector: FailureDetectorRegistry[Address],
    unreachableReaperInterval: FiniteDuration) extends Topic(groupRoutingLogic) {

  import context.dispatcher
  import me.atomd.things.mq.Aggregator._

  log.info("aggregator [{}] started", topic)

  val unreachableReaperTask = context.system.scheduler.schedule(unreachableReaperInterval, unreachableReaperInterval, self, ReapUnreachableTick)
  var reportingEntries: Map[Address, Any] = Map.empty

  override def isAggregator = true

  override def postStop(): Unit = {
    super.postStop()
    unreachableReaperTask.cancel()
  }

  override def receive = publishableBehavior orElse reportingBehavior

  def reportingBehavior: Receive = {
    case ReportingData(data: Any) => receiveReportingData(data)
    case ReapUnreachableTick      => reapUnreachable()
    case AskStats                 => sender() ! Stats(reportingEntries)
  }

  def receiveReportingData(data: Any): Unit = {
    val from = sender().path.address

    if (failureDetector.isMonitoring(from)) {
      log.info("Received reporting data from [{}]", from)
    } else {
      log.info("Received first reporting data from [{}]", from)
    }

    failureDetector.heartbeat(from)
    if (!reportingEntries.contains(from)) {
      publish(Available(from, data))
    }
    reportingEntries = reportingEntries.updated(from, data)
  }

  def reapUnreachable() {
    val (reachable, unreachable) = reportingEntries.partition { case (a, data) => failureDetector.isAvailable(a) }
    unreachable foreach {
      case (a, data) =>
        log.warning("Detected unreachable: [{}]", a)
        publish(Unreachable(a, data))
        failureDetector.remove(a)
    }
    reportingEntries = reachable
  }
}

object FailureDetectorLoader {

  def load(fqcn: String, config: Config, system: ActorSystem): FailureDetector = {
    system.asInstanceOf[ExtendedActorSystem].dynamicAccess.createInstanceFor[FailureDetector](
      fqcn, List(
        classOf[Config] -> config,
        classOf[EventStream] -> system.eventStream)).recover({
        case e => throw new ConfigurationException(
          s"Could not create custom failure detector [$fqcn] due to: ${e.toString}", e)
      }).get
  }

  def apply(fqcn: String, config: Config)(implicit ctx: ActorContext) = load(fqcn, config, ctx.system)

}

object Helpers {

  import java.util.concurrent.TimeUnit

  @inline final implicit class Requiring[A](val value: A) extends AnyVal {
    @inline def requiring(cond: Boolean, msg: ⇒ Any): A = {
      require(cond, msg)
      value
    }

    @inline def requiring(cond: A ⇒ Boolean, msg: ⇒ Any): A = {
      require(cond(value), msg)
      value
    }
  }

  final implicit class ConfigOps(val config: Config) extends AnyVal {
    def getMillisDuration(path: String): FiniteDuration = getDuration(path, TimeUnit.MILLISECONDS)

    def getNanosDuration(path: String): FiniteDuration = getDuration(path, TimeUnit.NANOSECONDS)

    private def getDuration(path: String, unit: TimeUnit): FiniteDuration =
      Duration(config.getDuration(path, unit), unit)
  }
}