package me.atomd.things.db

import akka.actor._
import me.atomd.things.ThingExtension

object TransientThing {
  def props(): Props = Props(classOf[TransientThing])
}

final class TransientThing() extends Thing with Actor with ActorLogging {

  def mediator = ThingExtension(context.system).topicRegion

  def isNotified = ThingExtension(context.system).Settings.enableNotification

  override def receive: Actor.Receive = {
    case request: Request =>
      key = request.key
      val handler = handleCommand orElse working
      context.become(handler)
      handler(request)
  }

  def handleCommand: Receive = {
    case Get(key)        => working(ValueGot)
    case Put(key, value) => working(ValuePut(value))
    case Remove(key)     => working(ValueRemoved)
    case AskStatus(key)  => working(ValueStatus)
  }
}
