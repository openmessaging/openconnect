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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Meta
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class Meta {
    /**
     * Maps Types to a list of Java classes that can be used to represent them.
     */
    private static final Map<Type, List<Class>> META_TYPE_CLASSES = new EnumMap<>(Type.class);
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();
    /**
     * Maps the Java classes to the corresponding Type.
     */
    private static final Map<Class<?>, Type> JAVA_CLASS_META_TYPES = new HashMap<>();

    static {
        META_TYPE_CLASSES.put(Type.INT8, Collections.singletonList((Class)Byte.class));
        META_TYPE_CLASSES.put(Type.INT16, Collections.singletonList((Class)Short.class));
        META_TYPE_CLASSES.put(Type.INT32, Collections.singletonList((Class)Integer.class));
        META_TYPE_CLASSES.put(Type.INT64, Collections.singletonList((Class)Long.class));
        META_TYPE_CLASSES.put(Type.FLOAT32, Collections.singletonList((Class)Float.class));
        META_TYPE_CLASSES.put(Type.FLOAT64, Collections.singletonList((Class)Double.class));
        META_TYPE_CLASSES.put(Type.BOOLEAN, Collections.singletonList((Class)Boolean.class));
        META_TYPE_CLASSES.put(Type.STRING, Collections.singletonList((Class)String.class));
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        META_TYPE_CLASSES.put(Type.BYTES, Arrays.asList((Class)byte[].class, (Class)ByteBuffer.class));
        META_TYPE_CLASSES.put(Type.ARRAY, Collections.singletonList((Class)List.class));
        META_TYPE_CLASSES.put(Type.MAP, Collections.singletonList((Class)Map.class));
        META_TYPE_CLASSES.put(Type.STRUCT, Collections.singletonList((Class)Struct.class));

        for (Map.Entry<Type, List<Class>> metaClasses : META_TYPE_CLASSES.entrySet()) {
            for (Class<?> metaClass : metaClasses.getValue()) {
                JAVA_CLASS_META_TYPES.put(metaClass, metaClasses.getKey());
            }
        }

        LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Collections.singletonList((Class)BigDecimal.class));
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Collections.singletonList((Class)java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Collections.singletonList((Class)java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Collections.singletonList((Class)java.util.Date.class));
        // We don't need to put these into JAVA_CLASS_META_TYPES since that's only used to determine metas for
        // metaless data and logical types will have ambiguous metas (e.g. many of them use the same Java class) so
        // they should not be used without metas.
    }

    public static Meta INT8_META = MetaBuilder.int8().build();
    public static Meta INT16_META = MetaBuilder.int16().build();
    public static Meta INT32_META = MetaBuilder.int32().build();
    public static Meta INT64_META = MetaBuilder.int64().build();
    public static Meta FLOAT32_META = MetaBuilder.float32().build();
    public static Meta FLOAT64_META = MetaBuilder.float64().build();
    public static Meta BOOLEAN_META = MetaBuilder.bool().build();
    public static Meta STRING_META = MetaBuilder.string().build();
    public static Meta BYTES_META = MetaBuilder.bytes().build();

    /**
     * The type of fields{@link Type}
     */
    protected final Type type;
    /**
     * Name of the meta.
     * Optional name, dataSource and version provide a built-in way to indicate what type of data is included.
     * Most useful for structs to indicate the semantics of the struct and map it to some existing underlying
     * serializer-specific schema. However, can also be useful in specifying other logical types (e.g. a set
     * is an array with additional constraints).
     */
    protected String name;
    /**
     * Data source information.
     */
    protected String dataSource;
    /**
     * Version of the meta.
     */
    protected Integer version;
    /**
     * Possible parameters.
     */
    protected Map<String, String> parameters;

    protected Integer hash = null;

    /**
     * Base construct.
     *
     * @param type       {@link Type}
     * @param name       Name of the meta.
     * @param version    Version of the meta.
     * @param dataSource Data source information.
     * @param parameters Possible parameters.
     */
    public Meta(Type type, String name, Integer version, String dataSource, Map<String, String> parameters) {
        if (null == type) {
            throw new RuntimeException("type cannot be null");
        }
        this.type = type;
        this.name = name;
        this.version = version;
        this.dataSource = dataSource;
        this.parameters = parameters;
    }

    /**
     * For map type only.Get {@link Meta} information about key.
     *
     * @return Returns meta information for map key.
     */
    public abstract Meta getKeyMeta();

    /**
     * Get {@link Meta} information about value only for map and array types.
     *
     * @return If the type is map, return the map-value's type; if the type is array, return the list-value's type
     */
    public abstract Meta getValueMeta();

    /**
     * For {@link Struct} type only.
     *
     * @return Returns the List<{@link Field}> corresponding to object[].
     */
    public abstract List<Field> getFields();

    /**
     * For {@link Struct} type only.
     *
     * @param fieldName The name of the field.
     * @return Get the field by field name.{@link Field}
     */
    public abstract Field getFieldByName(String fieldName);

    /**
     * Get the meta's {@link Type}.
     *
     * @return {@link Type}
     */
    public Type getType() {
        return this.type;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public static void validateValue(Meta meta, Object value) {
        validateValue(null, meta, value);
    }

    public static void validateValue(String name, Meta meta, Object value) {
        if (value == null) {
            throw new RuntimeException("Invalid value: null used for required field: \"" + name
                + "\", meta type: " + meta.getType());
        }

        List<Class> expectedClasses = expectedClassesFor(meta);

        if (expectedClasses == null) {
            throw new RuntimeException("Invalid Java object for meta type " + meta.getType()
                + ": " + value.getClass()
                + " for field: \"" + name + "\"");
        }

        boolean foundMatch = false;
        for (Class<?> expectedClass : expectedClasses) {
            if (expectedClass.isInstance(value)) {
                foundMatch = true;
                break;
            }
        }
        if (!foundMatch) {
            throw new RuntimeException("Invalid Java object for meta type " + meta.getType()
                + ": " + value.getClass()
                + " for field: \"" + name + "\"");
        }

        switch (meta.getType()) {
            case STRUCT:
                Struct struct = (Struct)value;
                if (!struct.meta().equals(meta)) {
                    throw new RuntimeException("Struct metas do not match.");
                }
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>)value;
                for (Object entry : array) {
                    validateValue(meta.getValueMeta(), entry);
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>)value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(meta.getKeyMeta(), entry.getKey());
                    validateValue(meta.getValueMeta(), entry.getValue());
                }
                break;
        }
    }

    private static List<Class> expectedClassesFor(Meta meta) {
        List<Class> expectedClasses = LOGICAL_TYPE_CLASSES.get(meta.getName());
        if (expectedClasses == null) {
            expectedClasses = META_TYPE_CLASSES.get(meta.getType());
        }
        return expectedClasses;
    }

    /**
     * Validate that the value can be used for this meta, i.e. that its type matches the scmetahema type and optional
     * requirements. Throws a DataException if the value is invalid.
     *
     * @param value the value to validate
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }

    /**
     * Get the {@link Type} associated with the given class.
     *
     * @param klass the Class to
     * @return the corresponding type, or null if there is no matching type
     */
    public static Type getMetaType(Class<?> klass) {
        synchronized (JAVA_CLASS_META_TYPES) {
            Type metaType = JAVA_CLASS_META_TYPES.get(klass);
            if (metaType != null) {
                return metaType;
            }

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_META_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_META_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }

    public Meta meta() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Meta meta = (Meta)o;
        return Objects.equals(dataSource, meta.dataSource) &&
            Objects.equals(name, meta.name) &&
            Objects.equals(type, meta.type) &&
            Objects.equals(parameters, meta.parameters);
    }

    @Override
    public int hashCode() {
        if (this.hash == null) {
            this.hash = Objects.hash(this.type, this.dataSource, this.name, this.parameters);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Meta{");
        if (this.name != null) {
            sb.append("name=").append(this.name).append(", ");
        }
        if (this.version != null) {
            sb.append("version=").append(this.version).append(", ");
        }
        sb.append("type=").append(this.type).append(", ");
        if (this.dataSource != null) {
            sb.append("dataSource=").append(this.dataSource).append(", ");
        }
        sb.append("parameters=").append(this.parameters).append("}");
        return sb.toString();
    }
}
