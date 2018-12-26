package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

/**
 * A connector is built at runtime through rest api.
 * A connector can holds multiple producer or consumer. Queue names is essential in configs.
 *
 */
public interface Connector {
    enum Status {
        STARTING,
        RUNNING,
        PAUSED,
        STOPPED,
        FAILED,
        DESTROYED
    }

    /**
     * Called when the connector clean starts.
     * @param config config passed by rest api. Config should include queue names.
     */
    public void onStart(KeyValue config);

    /**
     * When the task is paused.
     */
    public void onPause();

    /**
     * When the task is called stop.
     */
    public void onStop();

    /**
     * When the task throws unrecoverable exception and should be stopped.
     */
    public void onFailed(Exception e);

    /**
     * When the life cycle is about to end.
     */
    public void onDestroyed();
}
