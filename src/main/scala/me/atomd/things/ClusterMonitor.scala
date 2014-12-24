package me.atomd.things

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

object ClusterMonitor {
  def startMonitor(system: ActorSystem) = {
    val cluster = Cluster(system)
    val monitor = system.actorOf(Props[ClusterMonitor])
    cluster.subscribe(monitor, classOf[ClusterDomainEvent])
  }
}

class ClusterMonitor extends Actor with ActorLogging {
  def receive = {
    case state: CurrentClusterState           => log.info("Current state: {}", state)
    case MemberUp(member)                     => log.info("Member is up: {}, roles: {}", member, member.roles)
    case MemberRemoved(member, previousState) => log.info("Member removed: {}, roles: {}", member, member.roles)
    case MemberExited(member)                 => log.info("Member exited: {}, roles: {}", member, member.roles)
    case UnreachableMember(member)            => log.info("Member unreachable: {}, roles: {}", member, member.roles)
    case LeaderChanged(address)               => log.info("Leader changed: {}", address)
    case RoleLeaderChanged(role, member)      => log.info("Role {} leader changed: {}", role, member)
    case _: ClusterMetricsChanged             => // ignore
    case e: ClusterDomainEvent                => // ignore
  }
}
