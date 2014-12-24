package me.atomd.things

import akka.actor._

class RemoteAddressExtension(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}

object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtension]
