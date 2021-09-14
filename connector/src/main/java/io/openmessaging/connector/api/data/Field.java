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

package io.openmessaging.connector.api.data;

import java.util.Objects;

/**
 * Filed of the meta.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public class Field {

    /**
     * Index of a field. The index of fields in a meta should be unique and continuous。
     */
    private final int index;

    /**
     * The name of a field. Should be unique in a meta.
     */
    private final String name;

    /**
     * The type of the field.
     */
    private final Meta meta;

    public Field(int index, String name, Meta meta) {
        this.index = index;
        this.name = name;
        this.meta = meta;
    }

    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    public Meta meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(index, field.index) &&
            Objects.equals(name, field.name) &&
            Objects.equals(meta, field.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index, meta);
    }

    @Override
    public String toString() {
        return "Field{" +
            "name=" + name +
            ", index=" + index +
            ", meta=" + meta +
            "}";
    }
}
