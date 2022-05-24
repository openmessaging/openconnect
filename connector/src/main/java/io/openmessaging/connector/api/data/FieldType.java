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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Define the field type.
 */
public enum FieldType {

    /**
     * Byte Integer
     */
    INT8,

    /**
     * Short
     */
    INT16,

    /**
     * Integer
     */
    INT32,

    /**
     * Long
     */
    INT64,

    /**
     * Float
     */
    FLOAT32,

    /**
     * Double
     */
    FLOAT64,

    /**
     * Boolean
     */
    BOOLEAN,

    /**
     * String
     */
    STRING,

    /**
     * Byte
     */
    BYTES,

    /**
     * List
     */
    ARRAY,

    /**
     * Map
     */
    MAP,

    /**
     * Date
     */
    DATETIME,

    /**
     * Structured
     */
    STRUCT;

    /**
     * Maps Schema.Types to a list of Java classes that can be used to represent them.
     */
    public static final Map<FieldType, List<Class>> SCHEMA_TYPE_CLASSES = new EnumMap<>(FieldType.class);
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
    }

    /**
     * Determine whether it is a basic type
     *
     * @return
     */
    public boolean isPrimitive() {
        switch (this) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return true;
        }
        return false;
    }
}
