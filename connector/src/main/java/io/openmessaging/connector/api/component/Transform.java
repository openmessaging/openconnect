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

package io.openmessaging.connector.api.component;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;

/**
 * A Transform used to transform {@link ConnectRecord}.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public interface Transform<R extends ConnectRecord> extends Component {

    /**
     * Should invoke before start the connector.
     *
     * @param config component config
     */
    @Override
    default void validate(KeyValue config){

    }
    /**
     * transform record
     * @param record
     * @return
     */
    R doTransform(R record);
}
