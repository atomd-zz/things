package me.atomd.things.db

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Terminated

object LocalThingRegion {
  def props(thingProps: Props) = Props(classOf[LocalThingRegion], thingProps)
}

class LocalThingRegion(thingProps: Props) extends Actor with ActorLogging {

  def receive = {
    case request: Request =>
      context.child(request.key) match {
        case Some(ref) => ref forward request
        case None =>
          val connectSession = context.actorOf(thingProps, name = request.key)
          context.watch(connectSession)
          connectSession forward request
      }

    case Terminated(ref) => println("Terminated")
  }
}