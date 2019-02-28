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

package io.openmessaging.connector.api;

import java.util.Collection;
import java.util.Map;

/**
 * PositionStorageReader allow source task to access the position storage.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public interface PositionStorageReader {

    /**
     * Get the position for the specified partition.
     *
     * @param partition
     * @return
     */
    byte[] getPosition(byte[] partition);

    /**
     * Get a set of positions for the specified partitions.
     *
     * @param partitions
     * @return
     */
    Map<byte[], byte[]> getPositions(Collection<byte[]> partitions);
}
