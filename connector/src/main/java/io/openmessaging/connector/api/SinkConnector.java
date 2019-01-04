package io.openmessaging.connector.api;

import io.openmessaging.Message;

import java.util.Collection;

public abstract class SinkConnector implements Connector {
  /**
   * Receive messages from queue.
   * */
  public abstract void onReceive(Collection<Message> message);
}
