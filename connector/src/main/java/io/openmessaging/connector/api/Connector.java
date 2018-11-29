package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

import java.util.Map;

/** One connector per queue */
public interface Connector {
        public void onStart(KeyValue config) throws Exception;

        public void onStop();

        public void onPause();

}
