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

package io.openmessaging.connector.api.storage;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.Collection;
import java.util.Map;

public interface OffsetStorageReader {
    /**
     * Get the offset for the specified partition.
     *
     * @param partition record partition
     *
     * @return record offset
     */
     RecordOffset readOffset(RecordPartition partition);

    /**
     * Get the offset for the specified partitions.
     *
     * @param partitions record partitions
     *
     * @return record partition offset pair
     */
     Map<RecordPartition, RecordOffset> readOffsets(Collection<RecordPartition> partitions);

}
