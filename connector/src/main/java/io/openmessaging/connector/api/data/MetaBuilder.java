package io.openmessaging.connector.api.data;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * MetaBuilder provides a fluent API for constructing {@link Meta} objects. It allows you to set each of the
 * properties for the meta and each call returns the MetaBuilder so the calls can be chained. When nested
 * types are required, use one of the predefined metas from {@link Meta} or use a second MetaBuilder inline.
 * </p>
 * <p>
 * Here is an example of building a struct meta:
 * <pre>
 *     Meta dateMeta = MetaBuilder.struct()
 *         .name("com.example.CalendarDate").version(2)
 *         .field("month", Meta.STRING_META)
 *         .field("day", Meta.INT8_META)
 *         .field("year", Meta.INT16_META)
 *         .build();
 *     </pre>
 * </p>
 * <p>
 * Here is an example of using a second MetaBuilder to construct complex, nested types:
 * <pre>
 *     Meta userListMeta = MetaBuilder.array(
 *         MetaBuilder.struct().name("com.example.User").field("username", Meta.STRING_META).field("id", Meta
 *         .INT64_META).build()
 *     ).build();
 *     </pre>
 * </p>
 *
 * @author liuboyan
 */
public abstract class MetaBuilder {

    private static final String TYPE_FIELD = "type";
    private static final String NAME_FIELD = "name";
    private static final String DATASOURCE = "dataSource";

    private final Type type;
    private String name;
    private Integer version;
    private String dataSource;
    private Map<String, String> parameters;

    public MetaBuilder(Type type) {
        if (null == type) {
            throw new RuntimeException("type cannot be null");
        }
        this.type = type;
    }

    public Integer version() {
        return this.version;
    }

    public MetaBuilder version(Integer version) {
        this.version = version;
        return this;
    }

    public String name() {
        return this.name;
    }

    public MetaBuilder name(String name) {
        checkCanSet(NAME_FIELD, this.name, name);
        this.name = name;
        return this;
    }

    public String dataSource() {
        return this.dataSource;
    }

    public MetaBuilder dataSource(String dataSource) {
        checkCanSet(DATASOURCE, this.dataSource, dataSource);
        this.dataSource = dataSource;
        return this;
    }

    public Map<String, String> parameters() {
        return this.parameters == null ? null : Collections.unmodifiableMap(this.parameters);
    }

    public MetaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (this.parameters == null) {
            this.parameters = new LinkedHashMap<>();
        }
        this.parameters.put(propertyName, propertyValue);
        return this;
    }

    public MetaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty()) {
            return this;
        }
        if (this.parameters == null) {
            this.parameters = new LinkedHashMap<>();
        }
        this.parameters.putAll(props);
        return this;
    }

    public Type type() {
        return this.type;
    }

    public static MetaBuilder type(Type type) {
        switch (type) {
            case MAP:
                return new MetaMapBuilder(type, null, null);
            case ARRAY:
                return new MetaArrayBuilder(type, null);
            case STRUCT:
                return new MetaStructBuilder(type);
            default:
                return new MetaBaseBuilder(type);
        }
    }

    public abstract MetaBuilder field(String fieldName, Meta fieldMeta);

    public abstract MetaBuilder field(Field field);

    public abstract Meta keyMeta();

    public abstract Meta valueMeta();

    public abstract List<Field> fields();

    public abstract Field getFieldByName(String fieldName);

    public abstract Meta build();

    /**
     * Return a concrete instance of the {@link Meta} specified by this builder
     *
     * @return the {@link Meta}
     */
    public Meta meta() {
        return build();
    }

    private static void checkCanSet(String fieldName, Object fieldVal, Object val) {
        if (fieldVal != null && fieldVal != val) {
            throw new RuntimeException("Invalid MetaBuilder call: " + fieldName + " has already been set.");
        }
    }

    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null) {
            throw new RuntimeException(
                "Invalid MetaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
        }
    }

    // Basic types

    /**
     * @return a new {@link Type#INT8} MetaBuilder
     */
    public static MetaBuilder int8() {
        return new MetaBaseBuilder(Type.INT8);
    }

    /**
     * @return a new {@link Type#INT16} MetaBuilder
     */
    public static MetaBuilder int16() {
        return new MetaBaseBuilder(Type.INT16);
    }

    /**
     * @return a new {@link Type#INT32} MetaBuilder
     */
    public static MetaBuilder int32() {
        return new MetaBaseBuilder(Type.INT32);
    }

    /**
     * @return a new {@link Type#INT64} MetaBuilder
     */
    public static MetaBuilder int64() {
        return new MetaBaseBuilder(Type.INT64);
    }

    /**
     * @return a new {@link Type#FLOAT32} MetaBuilder
     */
    public static MetaBuilder float32() {
        return new MetaBaseBuilder(Type.FLOAT32);
    }

    /**
     * @return a new {@link Type#FLOAT64} MetaBuilder
     */
    public static MetaBuilder float64() {
        return new MetaBaseBuilder(Type.FLOAT64);
    }

    /**
     * @return a new {@link Type#BOOLEAN} MetaBuilder
     */
    public static MetaBuilder bool() {
        return new MetaBaseBuilder(Type.BOOLEAN);
    }

    /**
     * @return a new {@link Type#STRING} MetaBuilder
     */
    public static MetaBuilder string() {
        return new MetaBaseBuilder(Type.STRING);
    }

    /**
     * @return a new {@link Type#BYTES} MetaBuilder
     */
    public static MetaBuilder bytes() {
        return new MetaBaseBuilder(Type.BYTES);
    }

    // Structs

    /**
     * @return a new {@link Type#STRUCT} MetaBuilder
     */
    public static MetaBuilder struct() {
        return new MetaStructBuilder(Type.STRUCT);
    }

    // Arrays

    /**
     * @param valueMeta the meta for elements of the array
     * @return a new {@link Type#ARRAY} MetaBuilder
     */
    public static MetaBuilder array(Meta valueMeta) {
        if (null == valueMeta) {
            throw new RuntimeException("valueMeta cannot be null.");
        }
        MetaBuilder builder = new MetaArrayBuilder(Type.ARRAY, valueMeta);
        return builder;
    }

    // Maps

    /**
     * @param keyMeta   the meta for keys in the map
     * @param valueMeta the meta for values in the map
     * @return a new {@link Type#MAP} MetaBuilder
     */
    public static MetaBuilder map(Meta keyMeta, Meta valueMeta) {
        if (null == keyMeta) {
            throw new RuntimeException("keyMeta cannot be null.");
        }
        if (null == valueMeta) {
            throw new RuntimeException("valueMeta cannot be null.");
        }
        MetaBuilder builder = new MetaMapBuilder(Type.MAP, keyMeta, valueMeta);
        return builder;
    }
}
