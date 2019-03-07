package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

public interface Task {

  /**
   * Start the task with the given config.
   *
   * @param config the configuration information needed to start this task
   */
  void start(KeyValue config);

  /** Stop the task. */
  void stop();

  /** Pause the task. */
  void pause();

  /** Resume the task. */
  void resume();
}
