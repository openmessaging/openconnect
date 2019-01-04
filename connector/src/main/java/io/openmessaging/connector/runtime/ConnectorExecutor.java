package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.SinkConnector;
import io.openmessaging.connector.api.SourceConnector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConnectorExecutor implements Runnable {
  private ExecutorService executors = Executors.newCachedThreadPool();

  public void startConnector(Class<Connector> connectorClz, KeyValue config) throws Exception {
    Connector connector = connectorClz.newInstance();
    WorkerConnector workerConnector;
    if (connector instanceof SourceConnector) {
      workerConnector = new WorkerSourceConnector(connector, config);
    } else if (connector instanceof SinkConnector) {
      workerConnector = new WorkerSinkConnector(connector, config);
    } else {
      throw new RuntimeException("Neither source or sink connector.");
    }
    executors.submit(workerConnector);
  }

  @Override
  public void run() {}



}
