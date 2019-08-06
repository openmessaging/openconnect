/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.connector.api.header;


import java.util.Objects;

import io.openmessaging.connector.api.data.Meta;
import io.openmessaging.connector.api.data.MetaAndData;

/**
 * A {@link Header} implementation.
 */
public class DataHeader implements Header {

    private final String key;
    private final MetaAndData metaAndData;

    public DataHeader(String key, MetaAndData metaAndData) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.metaAndData = metaAndData;
    }

    public DataHeader(String key, Meta meta, Object value) {
        this(key,new MetaAndData(meta, value));
    }

    @Override
    public MetaAndData data() {
        return this.metaAndData;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public Header rename(String key) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        if (this.key.equals(key)) {
            return this;
        }
        return new DataHeader(key, this.metaAndData);
    }

    @Override
    public Header with(Meta meta, Object value) {
        return new DataHeader(key, this.metaAndData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.key, this.metaAndData);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Header) {
            Header that = (Header) obj;
            return Objects.equals(this.key, that.key())
                && Objects.equals(this.data(), that.data());
        }
        return false;
    }

    @Override
    public String toString() {
        return "DataHeader(key=" + key + ", value=" + data().getData() + ", meta=" + data().getMeta() + ")";
    }
}
