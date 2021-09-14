package io.openmessaging.connector.api.data;

import java.util.Locale;

/**
 * The type of a meta. These only include the core types;
 * logical types must be determined by checking the meta name.
 */
public enum Type {
    /**
     * 8-bit signed integer
     */
    INT8,
    /**
     * 16-bit signed integer
     */
    INT16,
    /**
     * 32-bit signed integer
     */
    INT32,
    /**
     * 64-bit signed integer
     */
    INT64,
    /**
     * BigInteger
     */
    BIG_INTEGER,
    /**
     * 32-bit IEEE 754 floating point number
     */
    FLOAT32,
    /**
     * 64-bit IEEE 754 floating point number
     */
    FLOAT64,
    /**
     * Boolean value (true or false)
     */
    BOOLEAN,
    /**
     * Character string that supports all Unicode characters.
     *
     * Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory representation.
     */
    STRING,
    /**
     * Sequence of unsigned 8-bit bytes
     */
    BYTES,
    /**
     * Date
     */
    DATETIME,

    /**
     * An ordered sequence of elements, each of which shares the same type.
     */
    ARRAY,
    /**
     * A mapping from keys to values. Both keys and values can be arbitrarily complex types, including complex types
     */
    MAP,
    /**
     * A structured record containing a set of named fields, each field using a fixed, independent.
     */
    STRUCT;

    private String name;

    Type() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return name;
    }

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
