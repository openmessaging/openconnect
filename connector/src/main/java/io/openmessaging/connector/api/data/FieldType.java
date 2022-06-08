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
