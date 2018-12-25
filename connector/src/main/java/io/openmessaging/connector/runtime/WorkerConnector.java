package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;

public abstract class WorkerConnector implements Runnable {
  private final Connector connector;
  private final KeyValue config;
  private Status status;

  enum Status {
    STARTING,
    RUNNING,
    PAUSED,
    STOPPED,
    FAILED
  }

  public WorkerConnector(Connector connector, KeyValue config) {
    this.connector = connector;
    this.config = config;
  }

  public abstract void init();

  public KeyValue getConfig() {
    return config;
  }

  @Override
  public void run() {
    try {
      status = Status.STARTING;
      connector.onStart(config);
      status = Status.RUNNING;

      while (status == Status.RUNNING) {
        doWork();
      }
      if (status == Status.PAUSED) {
        connector.onPause();
      }
    } catch (Exception e) {
      failed(e);
    } finally {
      connector.onStop();
    }
  }

  public abstract void doWork() throws Exception;

  public void pause() {
    this.status = Status.PAUSED;
  }

  public void stop() {
    this.status = Status.STOPPED;
  }

  public void failed(Exception e) {
    this.status = Status.FAILED;
  }
}
