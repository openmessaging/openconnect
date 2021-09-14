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
 */
package io.openmessaging.connector.api.common;

import java.util.Objects;

/**
 * Used for describe OpenMessaging queue metadata.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public class QueueMetaData {

    private String queueName;

    private String shardingKey;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    @Override
    public String toString() {
        return "QueueMetaData{" +
            "queueName='" + queueName + '\'' +
            ", shardingKey='" + shardingKey + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof QueueMetaData)) { return false; }
        QueueMetaData data = (QueueMetaData)o;
        return Objects.equals(queueName, data.queueName) &&
            Objects.equals(shardingKey, data.shardingKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueName, shardingKey);
    }
}
