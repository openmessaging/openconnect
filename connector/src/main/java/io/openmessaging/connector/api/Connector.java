package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;
import java.util.List;

public interface Connector {

    /**
     * Start the connector with the given config.
     */
    void start(KeyValue config);

    /**
     * Stop the connector.
     */
    void stop();

    /**
     * Returns the Task implementation for this Connector.
     * @return
     */
    Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration.
     * @return
     */
    List<KeyValue> taskConfigs();
}
