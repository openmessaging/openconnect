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

package io.openmessaging.connector.api.source;

import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.data.SourceDataEntry;
import java.util.Collection;

/**
 * SourceTask is a Task that pulls records from another system for storage in message queue.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class SourceTask implements Task {

    protected SourceTaskContext context;

    /**
     * Initialize this sourceTask.
     *
     * @param context
     */
    public void initialize(SourceTaskContext context) {
        this.context = context;
    }

    /**
     * Return a collection of message entries to send.
     */
    public abstract Collection<SourceDataEntry> poll();
}
