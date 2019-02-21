package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

public interface TaskContext {

    /**
     * Get the configurations of current task.
     * @return
     */
    KeyValue configs();
}
