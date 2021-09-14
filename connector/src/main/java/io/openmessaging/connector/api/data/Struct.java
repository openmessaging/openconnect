package io.openmessaging.connector.api.data;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A structured record containing a set of named fields with values, each field using an independent {@link Meta}.
 * Struct objects must specify a complete {@link Meta} up front, and only fields specified in the Meta may be set.
 *
 * @author liuboyan
 */
public class Struct {

    private final Meta meta;
    private final Object[] values;

    public Struct(Meta meta) {
        if (meta.getType() != Type.STRUCT) {
            throw new RuntimeException("meta type should be STRUCT.");
        }
        this.meta = meta;
        this.values = new Object[meta.getFields().size()];
    }

    public Meta meta() {
        return meta;
    }

    public Object get(String fieldName) {
        Field field = lookupField(fieldName);
        return get(field);
    }

    public Object get(Field field) {
        return values[field.index()];
    }

    public List<Field> getFields() {
        return this.meta.getFields();
    }

    // Note that all getters have to have boxed return types since the fields might be optional

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Byte.
     */
    public Byte getInt8(String fieldName) {
        return (Byte)getCheckType(fieldName, Type.INT8);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Short.
     */
    public Short getInt16(String fieldName) {
        return (Short)getCheckType(fieldName, Type.INT16);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Integer.
     */
    public Integer getInt32(String fieldName) {
        return (Integer)getCheckType(fieldName, Type.INT32);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Long.
     */
    public Long getInt64(String fieldName) {
        return (Long)getCheckType(fieldName, Type.INT64);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Float.
     */
    public Float getFloat32(String fieldName) {
        return (Float)getCheckType(fieldName, Type.FLOAT32);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Double.
     */
    public Double getFloat64(String fieldName) {
        return (Double)getCheckType(fieldName, Type.FLOAT64);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Boolean.
     */
    public Boolean getBoolean(String fieldName) {
        return (Boolean)getCheckType(fieldName, Type.BOOLEAN);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a String.
     */
    public String getString(String fieldName) {
        return (String)getCheckType(fieldName, Type.STRING);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a byte[].
     */
    public byte[] getBytes(String fieldName) {
        Object bytes = getCheckType(fieldName, Type.BYTES);
        if (bytes instanceof ByteBuffer) {
            return ((ByteBuffer)bytes).array();
        }
        return (byte[])bytes;
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a List.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getArray(String fieldName) {
        return (List<T>)getCheckType(fieldName, Type.ARRAY);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Map.
     */
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String fieldName) {
        return (Map<K, V>)getCheckType(fieldName, Type.MAP);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Struct.
     */
    public Struct getStruct(String fieldName) {
        return (Struct)getCheckType(fieldName, Type.STRUCT);
    }

    public Object getObject(String fieldName) {
        Field field = lookupField(fieldName);
        return values[field.index()];
    }

    /**
     * Set the value of a field. Validates the value, throwing a {@link RuntimeException} if it does not match the
     * field's
     * {@link Meta}.
     *
     * @param fieldName the name of the field to set
     * @param value     the value of the field
     * @return the Struct, to allow chaining of {@link #put(String, Object)} calls
     */
    public Struct put(String fieldName, Object value) {
        Field field = lookupField(fieldName);
        return put(field, value);
    }

    /**
     * Set the value of a field. Validates the value, throwing a {@link RuntimeException} if it does not match the
     * field's
     * {@link Meta}.
     *
     * @param field the field to set
     * @param value the value of the field
     * @return the Struct, to allow chaining of {@link #put(String, Object)} calls
     */
    public Struct put(Field field, Object value) {
        if (null == field) {
            throw new RuntimeException("field cannot be null.");
        }
        Meta.validateValue(field.name(), field.meta(), value);
        values[field.index()] = value;
        return this;
    }

    public MetaAndData toMetaData() {
        return new MetaAndData(this.meta, this);
    }

    /**
     * Validates that this struct has filled in all the necessary data with valid values. For required fields
     * without defaults, this validates that a value has been set and has matching types/metas. If any validation
     * fails, throws a DataException.
     */
    public void validate() {
        for (Field field : meta.getFields()) {
            Meta fieldMeta = field.meta();
            Object value = values[field.index()];
            Meta.validateValue(field.name(), fieldMeta, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Struct struct = (Struct)o;
        return Objects.equals(meta, struct.meta) &&
            Arrays.deepEquals(values, struct.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meta, Arrays.deepHashCode(values));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Struct{[");
        boolean first = true;
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null) {
                final Field field = meta.getFields().get(i);
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(field.toString()).append("=").append(value);
            }
        }
        return sb.append("]}").toString();
    }

    private Field lookupField(String fieldName) {
        Field field = meta.getFieldByName(fieldName);
        if (field == null) {
            throw new RuntimeException(fieldName + " is not a valid field name");
        }
        return field;
    }

    // Get the field's value, but also check that the field matches the specified type, throwing an exception if it
    // doesn't.
    // Used to implement the get*() methods that return typed data instead of Object
    private Object getCheckType(String fieldName, Type type) {
        Field field = lookupField(fieldName);
        if (field.meta().getType() != type) {
            throw new RuntimeException("Field '" + fieldName + "' is not of type " + type);
        }
        return values[field.index()];
    }

}
