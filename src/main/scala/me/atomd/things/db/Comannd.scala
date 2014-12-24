package me.atomd.things.db

sealed trait Command extends Serializable

sealed trait Response extends Serializable

sealed trait Request extends Serializable {
  def key: String
}

// Request
final case class Put(key: String, value: Any) extends Command with Request
final case class Get(key: String) extends Command with Request
final case class Remove(key: String) extends Command with Request
final case class AskStatus(key: String) extends Command with Request

// Response
final case class Result(value: Any) extends Command with Response
final case object Success extends Command with Response
final case object Failure extends Command with Response
final case class Status(key: String, Time: Long, location: String) extends Command with Response

sealed trait Event extends Serializable
case class ValuePut(value: Any) extends Event
case object ValueGot extends Event
case object ValueRemoved extends Event
case object ValueStatus extends Event

sealed trait Notification extends Serializable {
  def key: String
}
final case class Created(key: String, value: Any) extends Notification
final case class Removed(key: String) extends Notification
final case class Modified(key: String, value: Any) extends Notification
