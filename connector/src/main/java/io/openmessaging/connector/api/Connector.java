/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;
import java.util.List;

/**
 * Connector has the responsibility of manage the data integration between message queue and other system.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class Connector {

    protected ConnectorContext context;

    /**
     * Initialize this connector
     *
     * @param ctx ontext object used to interact with the Connect runtime
     */
    public void initialize(ConnectorContext ctx) {
        context = ctx;
    }

    /**
     * Should invoke before start the connector.
     *
     * @return error message
     */
    public abstract String verifyAndSetConfig(KeyValue config);

    /**
     * Start the connector with the given config.
     */
    public abstract void start();

    /**
     * Stop the connector.
     */
    public abstract void stop();

    /**
     * Pause the connector.
     */
    public abstract void pause();

    /**
     * Resume the connector.
     */
    public abstract void resume();

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return the implementation of the class
     */
    public abstract Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration.
     *
     * @return the configurations of the class
     */
    public abstract List<KeyValue> taskConfigs();
}
