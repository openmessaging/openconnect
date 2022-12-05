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
import java.util.Map;

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
     * @return SourceRecord list
     * @throws InterruptedException task thread interupt exception
     */
    public abstract List<SourceRecord> poll() throws InterruptedException;

    /**
     * batch commit
     * @param records
     * @param metadata
     */
    public void commit(final List<SourceRecord> records, Map<String,String> metadata) throws InterruptedException {
    }
    /**
     * commit record
     * @param record
     * @param metadata
     */
    public void commit(final SourceRecord record, Map<String,String> metadata) throws InterruptedException {

    }

    /**
     * If the user wants to use external storage to save the position,user can implement this
     * function.
     */
    public void commit() { }

    /**
     * return the metrics of source.
     * @return
     */
    public SourceMetrics getMetrics() {
        return null;
    }
}
