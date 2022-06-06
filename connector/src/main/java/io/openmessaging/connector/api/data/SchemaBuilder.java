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
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.SchemaBuilderException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * schema builder
 */
public class SchemaBuilder {
    private static final String TYPE_FIELD = "type";
    private static final String OPTIONAL_FIELD = "optional";
    private static final String DEFAULT_FIELD = "default";
    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";
    private static final String DOC_FIELD = "doc";

    /**
     * Name of the schema.
     */
    private String name;
    /**
     * version
     */
    private Integer version = null;
    /**
     * optional
     */
    private Boolean optional = null;
    /**
     * default value
     */
    private Object defaultValue = null;
    /**
     * Description schema
     */
    private String doc = null;

    /**
     * Type of the field
     */
    private FieldType type;

    /**
     * Structure of the schema, contains a list of {@link Field}
     */
    private Map<String, Field> fields;
    private Map<String, String> parameters;

    /**
     * map type
     */
    private Schema keySchema;
    private Schema valueSchema;


    public SchemaBuilder(FieldType type) {
        if (null == type) {
            throw new ConnectException("type cannot be null");
        }
        this.type = type;
        if (type == FieldType.STRUCT) {
            /**
             * ensure sequencing
             */
            fields = new LinkedHashMap<>();
        }
    }


    public boolean isOptional() {
        return this.optional == null ? true : optional;
    }


    /**
     * @return a new {@link FieldType#INT8} SchemaBuilder
     */
    public static SchemaBuilder int8() {
        return new SchemaBuilder(FieldType.INT8);
    }

    /**
     * @return a new {@link FieldType#INT16} SchemaBuilder
     */
    public static SchemaBuilder int16() {
        return new SchemaBuilder(FieldType.INT16);
    }

    /**
     * @return a new {@link FieldType#INT32} SchemaBuilder
     */
    public static SchemaBuilder int32() {
        return new SchemaBuilder(FieldType.INT32);
    }

    /**
     * @return a new {@link FieldType#INT64} SchemaBuilder
     */
    public static SchemaBuilder int64() {
        return new SchemaBuilder(FieldType.INT64);
    }

    /**
     * @return a new {@link FieldType#FLOAT32} SchemaBuilder
     */
    public static SchemaBuilder float32() {
        return new SchemaBuilder(FieldType.FLOAT32);
    }

    /**
     * @return a new {@link FieldType#FLOAT64} SchemaBuilder
     */
    public static SchemaBuilder float64() {
        return new SchemaBuilder(FieldType.FLOAT64);
    }

    /**
     * @return a new {@link FieldType#BOOLEAN} SchemaBuilder
     */
    public static SchemaBuilder bool() {
        return new SchemaBuilder(FieldType.BOOLEAN);
    }

    /**
     * @return a new {@link FieldType#STRING} SchemaBuilder
     */
    public static SchemaBuilder string() {
        return new SchemaBuilder(FieldType.STRING);
    }

    /**
     * @return a new {@link FieldType#BYTES} SchemaBuilder
     */
    public static SchemaBuilder bytes() {
        return new SchemaBuilder(FieldType.BYTES);
    }

    // Structs

    /**
     * @return a new {@link FieldType#STRUCT} SchemaBuilder
     */
    public static SchemaBuilder struct() {
        return new SchemaBuilder(FieldType.STRUCT);
    }


    // Maps & Arrays

    /**
     * @param valueSchema the schema for elements of the array
     */
    public static SchemaBuilder array(Schema valueSchema) {
        if (null == valueSchema) {
            throw new SchemaBuilderException("valueSchema cannot be null.");
        }
        SchemaBuilder builder = new SchemaBuilder(FieldType.ARRAY);
        builder.valueSchema = valueSchema;
        return builder;
    }

    /**
     * @param keySchema   the schema for keys in the map
     * @param valueSchema the schema for values in the map
     */
    public static SchemaBuilder map(Schema keySchema, Schema valueSchema) {
        if (null == keySchema) {
            throw new SchemaBuilderException("keySchema cannot be null.");
        }
        if (null == valueSchema) {
            throw new SchemaBuilderException("valueSchema cannot be null.");
        }
        SchemaBuilder builder = new SchemaBuilder(FieldType.MAP);
        builder.keySchema = keySchema;
        builder.valueSchema = valueSchema;
        return builder;
    }


    /**
     * date
     * @return
     */
    public static SchemaBuilder date() {
        return Date.builder();
    }

    /**
     * time
     * @return
     */
    public static SchemaBuilder time() {
        return Timestamp.builder();
    }

    /**
     * timestamp
     * @return
     */
    public static SchemaBuilder timestamp() {
        return Timestamp.builder();
    }

    /**
     * decimal
     * @param scale
     * @return
     */
    public static SchemaBuilder decimal(int scale) {
        return  Decimal.builder(scale);
    }



    /**
     * Add a field to this struct schema，ensure sequence
     */
    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
        if (type != FieldType.STRUCT) {
            throw new SchemaBuilderException("Cannot create fields on type " + type);
        }
        if (null == fieldName || fieldName.isEmpty()) {
            throw new SchemaBuilderException("fieldName cannot be null.");
        }
        if (null == fieldSchema) {
            throw new SchemaBuilderException("fieldSchema for field " + fieldName + " cannot be null.");
        }
        int fieldIndex = fields.size();
        if (fields.containsKey(fieldName)) {
            throw new SchemaBuilderException("Cannot create field because of field name duplication " + fieldName);
        }
        fields.put(fieldName, new Field(fieldIndex, fieldName, fieldSchema));
        return this;
    }


    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     */
    public List<Field> fields() {
        if (type != FieldType.STRUCT) {
            throw new ConnectException("Cannot list fields on non-struct type");
        }
        return new ArrayList<>(fields.values());
    }

    /**
     * get field by fieldName
     *
     * @param fieldName
     * @return
     */
    public Field field(String fieldName) {
        if (type != FieldType.STRUCT) {
            throw new ConnectException("Cannot look up fields on non-struct type");
        }
        return fields.get(fieldName);
    }

    /**
     * Sets the schema name
     *
     * @param name schema name
     * @return the current SchemaBuilder instance
     */
    public SchemaBuilder name(String name) {
        checkCanSet(NAME_FIELD, this.name, name);
        this.name = name;
        return this;
    }

    /**
     * Set the version of this schema. Schema versions are integers which, if provided, must indicate which schema is
     * newer and which is older by their ordering.
     */
    public SchemaBuilder version(Integer version) {
        checkCanSet(VERSION_FIELD, this.version, version);
        this.version = version;
        return this;
    }

    /**
     * Set the documentation for this schema.
     */
    public SchemaBuilder doc(String doc) {
        checkCanSet(DOC_FIELD, optional, true);
        this.doc = doc;
        return this;
    }

    /**
     * Set this schema as optional.
     */
    public SchemaBuilder optional() {
        checkCanSet(OPTIONAL_FIELD, optional, true);
        optional = true;
        return this;
    }

    /**
     * Set this schema as required.
     */
    public SchemaBuilder required() {
        checkCanSet(OPTIONAL_FIELD, optional, false);
        optional = false;
        return this;
    }

    public Map<String, String> parameters() {
        return parameters == null ? null : Collections.unmodifiableMap(parameters);
    }


    /**
     * Set a schema parameter
     * @param propertyName
     * @param propertyValue
     * @return
     */
    public SchemaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (parameters == null) {
            parameters = new LinkedHashMap<>();
        }
        parameters.put(propertyName, propertyValue);
        return this;
    }

    /**
     * Set schema parameters
     * @param props
     * @return
     */
    public SchemaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty()) {
            return this;
        }
        if (parameters == null) {
            parameters = new LinkedHashMap<>();
        }
        parameters.putAll(props);
        return this;
    }

    /**
     * Set the default value for this schema. The value is validated against the schema type, throwing a
     * {@link SchemaBuilderException} if it does not match.
     */
    public SchemaBuilder defaultValue(Object value) {
        checkCanSet(DEFAULT_FIELD, defaultValue, value);
        checkNotNull(TYPE_FIELD, type, DEFAULT_FIELD);
        defaultValue = value;
        return this;
    }


    private static void checkCanSet(String fieldName, Object fieldVal, Object val) {
        if (fieldVal != null && fieldVal != val) {
            throw new ConnectException("Invalid SchemaBuilder call: " + fieldName + " has already been set.");
        }
    }


    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null) {
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
        }
    }

    /**
     * Build the Schema using the current settings
     *
     * @return the {@link Schema}
     */
    public Schema build() {
        return new Schema(name, type, isOptional(), defaultValue, version, doc,
                fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields.values())), keySchema, valueSchema, parameters);
    }
}
