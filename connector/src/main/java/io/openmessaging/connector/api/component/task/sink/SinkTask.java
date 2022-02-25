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

package io.openmessaging.connector.api.component.task.sink;

import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.List;
import java.util.Map;

public abstract class SinkTask<R extends SinkTaskContext> implements Task<R> {

    protected R sinkTaskContext;

    @Override public void start(R sinkTaskContext) {
        this.sinkTaskContext = sinkTaskContext;
    }

    /**
     * Put the records to the sink
     *
     * @param sinkRecords
     */
    public abstract void put(List<ConnectRecord> sinkRecords) throws ConnectException;

    /**
     * Flush the records to the sink
     *
     * @param currentOffsets
     */
    public void flush(Map<RecordPartition, RecordOffset> currentOffsets) throws ConnectException {

    }

    /**
     * Will be invoked prior to an offset commit.
     *
     * @param currentOffsets
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by
     * queue that are safe to commit.
     */
    public Map<RecordPartition, RecordOffset> preCommit(Map<RecordPartition, RecordOffset> currentOffsets) {
        flush(currentOffsets);
        return currentOffsets;
    }
}
