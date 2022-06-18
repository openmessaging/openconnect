/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.connector.api.component.task.source;

import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.util.List;

/**
 * The source task API definition is used to define the logic for data pulling
 */
public abstract class SourceTask implements Task<SourceTaskContext> {

    protected SourceTaskContext sourceTaskContext;

    @Override
    public void init(SourceTaskContext sourceTaskContext) {
        this.sourceTaskContext = sourceTaskContext;
    }

    /**
     * Poll this source task for new records.
     *
     * @return connectRecord list
     * @throws InterruptedException task thread interupt exception
     */
    public abstract List<ConnectRecord> poll() throws InterruptedException;

    /**
     * <p>
     * Commit an individual {@link ConnectRecord} when the callback from the producer client is received.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality;Connect System will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     *
     * @throws InterruptedException task thread interupt exception
     * @param connectRecords connect records
     */
    public void commit(final List<ConnectRecord> connectRecords) throws InterruptedException {
        commit();
    }

    /**
     * If the user wants to use external storage to save the position,user can implement this
     * function.
     */
    public void commit() { }
}
