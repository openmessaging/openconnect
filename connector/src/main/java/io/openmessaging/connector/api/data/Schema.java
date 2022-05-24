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

import io.openmessaging.connector.api.errors.ConnectException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema
 */
public class Schema {

    /**
     * Name of the schema.
     */
    private String name;
    /**
     * schema version
     */
    private Integer version;
    /**
     * optional
     */
    private boolean optional;
    /**
     * default value
     */
    private Object defaultValue;
    /**
     * Description schema
     */
    private String doc;
    /**
     * Type of the field
     */
    private FieldType fieldType;
    /**
     * Structure of the schema, contains a list of {@link Field}
     */
    private List<Field> fields;
    private Map<String, Field> fieldsByName;

    /**
     * map type
     */
    private Schema keySchema;
    private Schema valueSchema;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public Schema(String name,FieldType fieldType, boolean optional, Object defaultValue, Integer version, String doc, List<Field> fields,Schema keySchema, Schema valueSchema) {
        this.fieldType = fieldType;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        if (this.fieldType == FieldType.STRUCT) {
            this.fields = fields == null ? Collections.<Field>emptyList() : fields;
            this.fieldsByName = new HashMap<>(this.fields.size());
            for (Field field : this.fields) {
                fieldsByName.put(field.getName(), field);
            }
        } else {
            this.fields = null;
            this.fieldsByName = null;
        }
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }


    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public Schema(String name,FieldType fieldType, boolean optional, Object defaultValue, Integer version, List<Field> fields) {
        this(name, fieldType, optional, defaultValue, version, null, fields, null, null);
    }


    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public Schema(String name, FieldType fieldType, List<Field> fields) {
        this(name, fieldType, false, null, null,  fields);
    }

    /**
     * get field
     * @param fieldName
     * @return
     */
    public Field getField(String fieldName) {
        if (fieldsByName.containsKey(fieldName)){
            return fieldsByName.get(fieldName);
        }
        return null;
    }

    /**
     * add field
     * @param field
     */
    public void addField(Field field) {
        this.fields.add(field);
        this.fieldsByName.put(field.getName(), field);
    }

    public Schema keySchema() {
        return keySchema;
    }
    public Schema valueSchema() {
        return valueSchema;
    }

    public boolean isOptional() {
        return optional;
    }

    public Object defaultValue(){
        return defaultValue;
    }

    /**
     * Get the optional version of the schema. If a version is included, newer versions *must* be larger than older ones.
     * @return the version of this schema
     */
    public Integer version(){
        return version;
    }

    /**
     * @return the documentation for this schema
     */
    public String doc() {
        return doc;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    /**
     * Validate that the value can be used for this schema
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }
    /**
     * Validate that the value can be used with the schema
     */
    public static void validateValue(Schema schema, Object value) {
        validateValue(null, schema, value);
    }
    public static void validateValue(String name, Schema schema, Object value) {

        // check optional
        if (value == null) {
            if (!schema.isOptional()) {
                throw new ConnectException("Invalid value: null used for required field: \"" + name
                        + "\", schema type: " + schema.getFieldType());
            }
            return;
        }

        // check field type
        List<Class> expectedClasses = expectedClassesFor(schema);

        if (expectedClasses == null) {
            throw new ConnectException("Invalid Java object for schema type " + schema.getFieldType()
                    + ": " + value.getClass()
                    + " for field: \"" + name + "\"");
        }

        boolean foundMatch = false;
        if (expectedClasses.size() == 1) {
            foundMatch = expectedClasses.get(0).isInstance(value);
        } else {
            for (Class<?> expectedClass : expectedClasses) {
                if (expectedClass.isInstance(value)) {
                    foundMatch = true;
                    break;
                }
            }
        }

        if (!foundMatch) {
            throw new ConnectException("Invalid Java object for schema type " + schema.getFieldType()
                    + ": " + value.getClass()
                    + " for field: \"" + name + "\"");
        }

        switch (schema.getFieldType()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema)) {
                    throw new ConnectException("Struct schemas do not match.");
                }
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                for (Object entry : array) {
                    validateValue(schema.valueSchema(), entry);
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.keySchema(), entry.getKey());
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    /**
     * expected classes for
     * @param schema
     * @return
     */
    private static List<Class> expectedClassesFor(Schema schema) {
        return FieldType.SCHEMA_TYPE_CLASSES.get(schema.getFieldType());
    }

    @Override
    public String toString() {
        return "Schema{" +
                "name='" + name + '\'' +
                ", version=" + version +
                ", optional=" + optional +
                ", defaultValue=" + defaultValue +
                ", doc='" + doc + '\'' +
                ", fieldType=" + fieldType +
                ", fields=" + fields +
                ", keySchema=" + keySchema +
                ", valueSchema=" + valueSchema +
                '}';
    }
}
