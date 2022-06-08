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

package io.openmessaging.connector.api.data;

import java.util.Objects;

/**
 * Filed of the schema.
 */
public class Field {

    private int index;

    /**
     * The name of a file. Should be unique in a shcema.
     */
    private String name;

    /**
     * The type of the file.
     */
    private Schema schema;

    public Field(int index, String name, Schema schema) {

        this.index = index;
        this.name = name;
        this.schema = schema;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Field)) return false;
        Field field = (Field) o;
        return getIndex() == field.getIndex() && Objects.equals(getName(), field.getName()) && Objects.equals(getSchema(), field.getSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIndex(), getName(), getSchema());
    }

    @Override public String toString() {
        return "Field{" +
            "index=" + index +
            ", name='" + name + '\'' +
            ", schema=" + schema +
            '}';
    }
}
