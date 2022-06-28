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

package io.openmessaging.connector.api.storage;


import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;

import java.util.Map;

/**
 * position storage writer
 */
public interface  OffsetStorageWriter {

    /**
     * write offset
     * @param partition partition
     * @param position position
     */
   void writeOffset(RecordPartition partition, RecordOffset position);


    /**
     * write offsets
     * @param positions positions
     */
    void writeOffset(Map<RecordPartition, RecordOffset> positions);
}
