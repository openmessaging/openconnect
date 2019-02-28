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

import io.openmessaging.connector.api.TaskContext;
import java.util.List;
import java.util.Map;

/**
 * A context allow sink task to access the runtime.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public interface SinkTaskContext extends TaskContext {

    /**
     * Reset the consumer offset for the given queue.
     *
     * @param queueName
     * @param offset
     */
    void resetOffset(String queueName, Long offset);

    /**
     * Reset the consumer offsets for the given queue.
     *
     * @param offsets
     */
    void resetOffset(Map<String, Long> offsets);

    /**
     * Pause consumption of messages from the specified TopicPartitions.
     *
     * @param queueName
     */
    void pause(List<String> queueName);

    /**
     * Resume consumption of messages from previously paused TopicPartitions.
     *
     * @param queueName
     */
    void resume(List<String> queueName);
}
