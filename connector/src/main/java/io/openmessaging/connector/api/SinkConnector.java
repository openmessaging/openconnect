package io.openmessaging.connector.api;

import io.openmessaging.Message;

import java.util.Collection;

public abstract class SinkConnector implements Connector {

  public abstract void onReceive(Message message);
}
