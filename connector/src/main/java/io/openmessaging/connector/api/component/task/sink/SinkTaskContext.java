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

import io.openmessaging.connector.api.component.ComponentContext;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SinkTaskContext extends ComponentContext {

    /**
     * Get the Connector Name
     *
     * @return connector name
     */
    String getConnectorName();

    /**
     * Get the Task Name of connector.
     *
     * @return task name
     */
    String getTaskName();

    /**
     * Reset the consumer offset for the given queue.
     *
     * @param recordPartition the partition to reset offset.
     * @param recordOffset    the offset to reset to.
     */
    void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset);

    /**
     * Reset the offsets for the given partition.
     *
     * @param offsets the map of offsets for targetPartition.
     */
    void resetOffset(Map<RecordPartition, RecordOffset> offsets);

    /**
     * Pause consumption of messages from the specified partition.
     *
     * @param partitions the partition list to be reset offset.
     */
    void pause(List<RecordPartition> partitions);

    /**
     * Resume consumption of messages from previously paused Partition.
     *
     * @param partitions the partition list to be resume.
     */
    void resume(List<RecordPartition> partitions);

    /**
     * Current task assignment processing partition
     *
     * @return the partition list
     */
    Set<RecordPartition> assignment();
}
