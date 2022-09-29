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

import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schema
 */
public class Schema {

    /**
     * Maps Schema.Types to a list of Java classes that can be used to represent them.
     */
    public static final Map<FieldType, List<Class>> SCHEMA_TYPE_CLASSES = new EnumMap<>(FieldType.class);

    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the Java classes to the corresponding Schema.Type.
     */
    public static final Map<Class<?>, FieldType> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        SCHEMA_TYPE_CLASSES.put(FieldType.INT8, Collections.singletonList((Class) Byte.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.INT16, Collections.singletonList((Class) Short.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.INT32, Collections.singletonList((Class) Integer.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.INT64, Collections.singletonList((Class) Long.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.FLOAT32, Collections.singletonList((Class) Float.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.FLOAT64, Collections.singletonList((Class) Double.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.BOOLEAN, Collections.singletonList((Class) Boolean.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.STRING, Collections.singletonList((Class) String.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.ARRAY, Collections.singletonList((Class) List.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.MAP, Collections.singletonList((Class) Map.class));
        SCHEMA_TYPE_CLASSES.put(FieldType.STRUCT, Collections.singletonList((Class) Struct.class));

        for (Map.Entry<FieldType, List<Class>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
            for (Class<?> schemaClass : schemaClasses.getValue()) {
                JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
            }
        }

        LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Collections.singletonList((Class) BigDecimal.class));
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class));

    }


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
    private Map<String, String> parameters;
    /**
     * map type
     */
    private Schema keySchema;
    private Schema valueSchema;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public Schema(String name,FieldType fieldType, boolean optional, Object defaultValue, Integer version, String doc, List<Field> fields,Schema keySchema, Schema valueSchema, Map<String, String> parameters) {
        this.fieldType = fieldType;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        this.parameters = parameters;
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
     *
     * @param defaultValue defaultValue
     * @param fields fields
     * @param fieldType fieldType
     * @param name name
     * @param optional optional
     * @param version version
     */
    public Schema(String name,FieldType fieldType, boolean optional, Object defaultValue, Integer version, List<Field> fields) {
        this(name, fieldType, optional, defaultValue, version, null, fields, null, null,new ConcurrentHashMap<>());
    }


    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     *
     * @param name name
     * @param fieldType fieldType
     * @param fields fields
     */
    public Schema(String name, FieldType fieldType, List<Field> fields) {
        this(name, fieldType, false, null, null,  fields);
    }

    // start getter and setter
    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    public Map<String, Field> getFieldsByName() {
        return fieldsByName;
    }

    public void setFieldsByName(Map<String, Field> fieldsByName) {
        this.fieldsByName = fieldsByName;
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
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

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    // end getter and setter

    /**
     * get field
     * @param fieldName field name
     * @return field
     */
    public Field getField(String fieldName) {
        if (fieldsByName.containsKey(fieldName)){
            return fieldsByName.get(fieldName);
        }
        return null;
    }

    /**
     * add field
     * @param field field
     */
    public void addField(Field field) {
        if (this.fields == null) {
            this.fields = new ArrayList<>();
        }
        if (this.fieldsByName == null) {
            this.fieldsByName = new HashMap<>();
        }
        this.fields.add(field);
        this.fieldsByName.put(field.getName(), field);
    }

    /**
     * Validate that the value can be used for this schema
     * @param value value
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }
    /**
     * Validate that the value can be used with the schema
     * @param schema schema
     * @param value value
     */
    public static void validateValue(Schema schema, Object value) {
        validateValue(null, schema, value);
    }

    /**
     * Validate that the value can be used with the schema
     *
     * @param name name
     * @param schema schema
     * @param value value
     */
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
                    validateValue(schema.getValueSchema(), entry);
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.getKeySchema(), entry.getKey());
                    validateValue(schema.getValueSchema(), entry.getValue());
                }
                break;
        }
    }

    /**
     * expected classes for
     * @param schema schema
     * @return class list
     */
    private static List<Class> expectedClassesFor(Schema schema) {
        List<Class> expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.getName());
        if (expectedClasses == null) {
            expectedClasses = SCHEMA_TYPE_CLASSES.getOrDefault(schema.getFieldType(), Collections.emptyList());
        }
        return expectedClasses;
    }


    /**
     *  Get the type associated with the given class.
     * @param klass class
     * @return field type
     */
    public static FieldType schemaType(Class<?> klass) {
        synchronized (JAVA_CLASS_SCHEMA_TYPES) {
            FieldType schemaType = JAVA_CLASS_SCHEMA_TYPES.get(klass);
            if (Objects.nonNull(schemaType)) {
                return schemaType;
            }
            for (Map.Entry<Class<?>, FieldType> entry : JAVA_CLASS_SCHEMA_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    JAVA_CLASS_SCHEMA_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                }
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Schema)) return false;
        Schema schema = (Schema) o;
        return isOptional() == schema.isOptional() && Objects.equals(getName(), schema.getName()) && Objects.equals(getVersion(), schema.getVersion()) && Objects.equals(getDefaultValue(), schema.getDefaultValue()) && Objects.equals(getDoc(), schema.getDoc()) && getFieldType() == schema.getFieldType() && Objects.equals(getFields(), schema.getFields()) && Objects.equals(getParameters(), schema.getParameters()) && Objects.equals(getKeySchema(), schema.getKeySchema()) && Objects.equals(getValueSchema(), schema.getValueSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getVersion(), isOptional(), getDefaultValue(), getDoc(), getFieldType(), getFields(), getParameters(), getKeySchema(), getValueSchema());
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
                ", fieldsByName=" + fieldsByName +
                ", parameters=" + parameters +
                ", keySchema=" + keySchema +
                ", valueSchema=" + valueSchema +
                '}';
    }
}
