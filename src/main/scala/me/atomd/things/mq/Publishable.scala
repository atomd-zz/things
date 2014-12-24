package me.atomd.things.mq

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.SubscribeAck
import akka.contrib.pattern.DistributedPubSubMediator.Unsubscribe
import akka.contrib.pattern.DistributedPubSubMediator.UnsubscribeAck
import akka.event.LoggingAdapter
import akka.routing.ActorRefRoutee
import akka.routing.Router

trait Publishable { _: Actor =>

  var queues = Set[ActorRef]()
  // ActorRef of queue
  var groupToQueues: Map[Option[String], Set[ActorRefRoutee]] = Map.empty.withDefaultValue(Set.empty)

  def log: LoggingAdapter

  def groupRouter: Router

  def topic = self.path.name

  def publishableBehavior: Receive = {
    case x @ Subscribe(topic, group, queue) =>
      insertSubscription(group, queue)
      sender() ! SubscribeAck(x)
      log.info("{} successfully subscribed to topic(me) [{}] under group [{}]", queue, topic, group)

    case x @ Unsubscribe(topic, group, queue) =>
      removeSubscription(group, queue)
      sender() ! UnsubscribeAck(x)
      log.info("{} successfully unsubscribed to topic(me) [{}] under group [{}]", queue, topic, group)

    case Publish(topic, msg, _) => publish(msg)

    case Terminated(ref)        => removeSubscription(ref)
  }

  def publish(x: Any) {
    groupToQueues foreach {
      case (None, queues) => queues foreach (_.ref ! x)
      case (_, queues)    => groupRouter.withRoutees(queues.toVector).route(x, self)
    }
  }

  def existsQueue(queue: ActorRef) = {
    groupToQueues exists { case (group, queues) => queues.contains(ActorRefRoutee(queue)) }
  }

  def insertSubscription(group: Option[String], queue: ActorRef) {
    if (!queues.contains(queue)) {
      context watch queue
      queues += queue
    }
    groupToQueues = groupToQueues.updated(group, groupToQueues(group) + ActorRefRoutee(queue))
  }

  def removeSubscription(group: Option[String], queue: ActorRef) {
    if (!existsQueue(queue)) {
      context unwatch queue
      queues -= queue
    }
    groupToQueues = groupToQueues.updated(group, groupToQueues(group) - ActorRefRoutee(queue))
  }

  def removeSubscription(queue: ActorRef) {
    context unwatch queue
    queues -= queue
    groupToQueues = for {
      (group, queues) <- groupToQueues
    } yield (group -> (queues - ActorRefRoutee(queue)))
  }
}
