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

import java.util.List;

/**
 * Schema
 */
public class Schema {

    /**
     * Name of the schema.
     */
    private String name;

    /**
     * Type of the field
     */
    private FieldType fieldType;
    /**
     * Structure of the schema, contains a list of {@link Field}
     */
    private List<Field> fields;

    public Schema(String name, FieldType fieldType, List<Field> fields) {
        this.name = name;
        this.fieldType = fieldType;
        this.fields = fields;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public Field getField(String fieldName) {

        for (Field field : fields) {
            if (field.getName()
                .equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    public void addField(Field field) {
        this.fields.add(field);
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    @Override public String toString() {
        return "Schema{" +
            "name='" + name + '\'' +
            ", fieldType=" + fieldType +
            ", fields=" + fields +
            '}';
    }
}
