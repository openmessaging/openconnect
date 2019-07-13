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

package io.openmessaging.connector.api.sink;

import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.SinkDataEntry;
import java.util.Collection;
import java.util.Map;

/**
 * SinkTask is a task takes from message queue and send them to another system.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class SinkTask implements Task {

    protected SinkTaskContext context;

    /**
     * Initialize this sinkTask.
     *
     * @param context the context of current task.
     */
    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Put the data entries to the sink.
     */
    public abstract void put(Collection<SinkDataEntry> sinkDataEntries);

    /**
     * Commit all records that have been {@link #put(Collection)} for the specified queue.
     *
     * @param offsets
     */
    public abstract void commit(Map<QueueMetaData, Long> offsets);

    /**
     * Will be invoked prior to an offset commit.
     *
     * @param currentOffsets
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by
     * queue that are safe to commit.
     */
    public Map<QueueMetaData, Long> preCommit(Map<QueueMetaData, Long> currentOffsets) {
        commit(currentOffsets);
        return currentOffsets;
    }
}
