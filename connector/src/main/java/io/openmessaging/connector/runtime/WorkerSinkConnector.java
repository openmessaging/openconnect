package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.SinkConnector;
import io.openmessaging.consumer.PullConsumer;

public class WorkerSinkConnector extends WorkerConnector {
  private PullConsumer consumer;
  private MessagingAccessPoint map;
  private SinkConnector connector;

  public WorkerSinkConnector(Connector connector, KeyValue config) {
    super(connector, config);
    this.connector = (SinkConnector) connector;
  }

  @Override
  public void init() {
    map = OMS.getMessagingAccessPoint(getConfig().getString("AccessPoint"));
    consumer = map.createPullConsumer();
    consumer.attachQueue(getConfig().getString("Queue"));
  }

  @Override
  public void doWork() throws Exception {
    connector.onReceive(consumer.receive());
  }
}
