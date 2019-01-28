package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

public interface Task {

    /**
     * Start the task with the given config.
     * @param config
     */
    void start(KeyValue config);

    /**
     * Stop the task.
     */
    void stop();
}
