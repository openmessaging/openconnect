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
package io.openmessaging.connector.api.component.connector;

import io.openmessaging.connector.api.component.ComponentContext;

public interface ConnectorContext extends ComponentContext {
    /**
     * Requests that the runtime reconfigure the Tasks for this source. This should be used to indicate to the runtime
     * that something about the input/output has changed and the running Tasks will need to be modified.
     */
    void requestTaskReconfiguration();

    /**
     * Raise an unrecoverable exception to the Connect framework. This will cause the status of the connector to
     * transition to FAILED.
     *
     * @param e Exception to be raised.
     */
    void raiseError(Exception e);
}
