package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.SourceConnector;
import io.openmessaging.producer.BatchMessageSender;
import io.openmessaging.producer.Producer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WorkerSourceConnector extends WorkerConnector {
  private Producer producer;
  private BatchMessageSender sender;
  private MessagingAccessPoint map;
  private SourceConnector connector;
  private ScheduledExecutorService committer = Executors.newScheduledThreadPool(1);

  public WorkerSourceConnector(Connector connector, KeyValue config) {
    super(connector, config);
    this.connector = (SourceConnector) connector;
    committer.schedule(
        new Runnable() {
          @Override
          public void run() {
            sender.commit();
          }
        },
        config.getLong("commit.interval.ms"),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void init() {
    map = OMS.getMessagingAccessPoint(getConfig().getString("AccessPoint"));
    sender = producer.createBatchMessageSender();
  }

  @Override
  public void doWork() throws Exception {
    for (Message message : connector.poll()) {
      sender.send(message);
    }
  }
}
