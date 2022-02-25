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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.openmessaging.connector.api.component.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Component;
import io.openmessaging.connector.api.component.task.Task;
import java.util.List;

public abstract class Connector implements Component<ConnectorContext> {

    protected ConnectorContext connectorContext;

    @Override public void start(ConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
    }

    /**
     * Pause the connector.
     */
    public abstract void pause();

    /**
     * Resume the connector.
     */
    public abstract void resume();

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    public abstract List<KeyValue> taskConfigs(int maxTasks);

    /**
     * Return the current connector class
     *
     * @return
     */
    public abstract Class<? extends Task> taskClass();

}
