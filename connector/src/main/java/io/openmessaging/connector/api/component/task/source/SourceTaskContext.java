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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.ComponentContext;
import io.openmessaging.connector.api.storage.OffsetStorageReader;

public interface SourceTaskContext extends ComponentContext {

    /**
     * Get the OffsetStorageReader for this SourceTask.
     *
     * @return offset storage reader
     */
    OffsetStorageReader offsetStorageReader();

    /**
     * Get the Connector Name
     *
     * @return connector name
     */
    String getConnectorName();

    /**
     * Get the Task Id of connector.
     *
     * @return task name
     */

    String getTaskName();

    /**
     * Get the configurations of current task.
     *
     * @return the configuration of current task.
     */
    KeyValue configs();
}
