package io.openmessaging.connector.api;

import io.openmessaging.Message;

import java.util.Collection;

public abstract class SourceConnector implements Connector {
  public abstract Collection<Message> poll();
}

