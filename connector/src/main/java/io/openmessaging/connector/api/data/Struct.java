
package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.errors.ConnectException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * struct object model
 */
public class Struct {

    private final Schema schema;
    private final Object[] values;

    /**
     * construct
     * @param schema
     */
    public Struct(Schema schema) {
        if (schema.getFieldType() != FieldType.STRUCT) {
            throw new ConnectException("Not a struct schema: " + schema);
        }
        this.schema = schema;
        this.values = new Object[schema.getFields().size()];
    }

    /**
     * get schema
     * @return
     */
    public Schema schema() {
        return schema;
    }

    /**
     * get data by fieldName
     * @param fieldName
     * @return
     */
    public Object get(String fieldName) {
        Field field = lookupField(fieldName);
        return get(field);
    }

    /**
     * get data by Field object
     * @param field
     * @return
     */
    public Object get(Field field) {
        Object val = values[field.getIndex()];
        if (val == null && field.getSchema().defaultValue() != null) {
            val = field.getSchema().defaultValue();
        }
        return val;
    }


    public Object getWithoutDefault(String fieldName) {
        Field field = lookupField(fieldName);
        return values[field.getIndex()];
    }

    /**
     * cast the result to a Byte.
     * @param fieldName
     * @return
     */
    public Byte getInt8(String fieldName) {
        return (Byte) getCheckType(fieldName, FieldType.INT8);
    }

    /**
     * cast the result to a Integer.
     * @param fieldName
     * @return
     */
    public Integer getInt32(String fieldName) {
        return (Integer) getCheckType(fieldName, FieldType.INT32);
    }

    /**
     * cast the result to a Long.
     * @param fieldName
     * @return
     */
    public Long getInt64(String fieldName) {
        return (Long) getCheckType(fieldName, FieldType.INT64);
    }

    /**
     * cast the result to a Float.
     * @param fieldName
     * @return
     */
    public Float getFloat32(String fieldName) {
        return (Float) getCheckType(fieldName, FieldType.FLOAT32);
    }

    /**
     * cast the result to a Double.
     * @param fieldName
     * @return
     */
    public Double getFloat64(String fieldName) {
        return (Double) getCheckType(fieldName, FieldType.FLOAT64);
    }

    /**
     * cast the result to a Boolean.
     * @param fieldName
     * @return
     */
    public Boolean getBoolean(String fieldName) {
        return (Boolean) getCheckType(fieldName, FieldType.BOOLEAN);
    }

    /**
     * cast the result to a String.
     * @param fieldName
     * @return
     */
    public String getString(String fieldName) {
        return (String) getCheckType(fieldName, FieldType.STRING);
    }

    /**
     *  cast the result to a byte[].
     * @param fieldName
     * @return
     */
    public byte[] getBytes(String fieldName) {
        Object bytes = getCheckType(fieldName, FieldType.BYTES);
        if (bytes instanceof ByteBuffer) {
            return ((ByteBuffer) bytes).array();
        }
        return (byte[]) bytes;
    }

    /**
     *  cast the result to a List.
     * @param fieldName
     * @param <T>
     * @return
     */
    public <T> List<T> getArray(String fieldName) {
        return (List<T>) getCheckType(fieldName, FieldType.ARRAY);
    }

    /**
     * cast the result to a Map.
     */
    public <K, V> Map<K, V> getMap(String fieldName) {
        return (Map<K, V>) getCheckType(fieldName, FieldType.MAP);
    }

    /**
     * cast the result to a Struct.
     */
    public Struct getStruct(String fieldName) {
        return (Struct) getCheckType(fieldName, FieldType.STRUCT);
    }


    /**
     * Set value by fieldName
     * @param fieldName
     * @param value
     * @return
     */
    public Struct put(String fieldName, Object value) {
        Field field = lookupField(fieldName);
        return put(field, value);
    }

    /**
     * set data
     * @param field
     * @param value
     * @return
     */
    public Struct put(Field field, Object value) {
        if (null == field) {
            throw new ConnectException("field cannot be null.");
        }
        Schema.validateValue(field.getName(), field.getSchema(), value);
        values[field.getIndex()] = value;
        return this;
    }


    /**
     *  Validates that this struct has filled in all the necessary data with valid values
     */
    public void validate() {
        for (Field field : schema.getFields()) {
            Schema fieldSchema = field.getSchema();
            Object value = values[field.getIndex()];
            if (value == null && (fieldSchema.isOptional() || fieldSchema.defaultValue() != null)) {
                continue;
            }
            Schema.validateValue(field.getName(), fieldSchema, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Struct struct = (Struct) o;
        return Objects.equals(schema, struct.schema) &&
                Arrays.deepEquals(values, struct.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, Arrays.deepHashCode(values));
    }

    private Field lookupField(String fieldName) {
        Field field = schema.getField(fieldName);
        if (field == null) {
            throw new ConnectException(fieldName + " is not a valid field name");
        }
        return field;
    }

    private Object getCheckType(String fieldName, FieldType type) {
        Field field = lookupField(fieldName);
        if (field.getSchema().getFieldType() != type) {
            throw new ConnectException("Field '" + fieldName + "' is not of type " + type);
        }
        return values[field.getIndex()];
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Struct{");
        boolean first = true;
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null) {
                final Field field = schema.getFields().get(i);
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(field.getName()).append("=").append(value);
            }
        }
        return sb.append("}").toString();
    }

}

