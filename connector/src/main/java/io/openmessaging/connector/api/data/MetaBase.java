package io.openmessaging.connector.api.data;

import java.util.List;
import java.util.Map;

/**
 * Meta implement for base types.
 * {@link Type}.INT8 {@link Type}.INT16 {@link Type}.INT32 {@link Type}.INT64
 * {@link Type}.BIG_INTEGER {@link Type}.FLOAT32 {@link Type}.FLOAT64 {@link Type}.BOOLEAN
 * {@link Type}.STRING {@link Type}.BYTES {@link Type}.DATETIME
 *
 * @author liuboyan
 */
public class MetaBase extends Meta {
    /**
     * Base construct.
     *
     * @param type       {@link Type}
     * @param name       Name of the meta.
     * @param version    Version of the meta.
     * @param dataSource Data source information.
     * @param parameters Possible parameters.
     */
    public MetaBase(Type type, String name, Integer version, String dataSource, Map<String, String> parameters) {
        super(type, name, version, dataSource, parameters);
    }

    @Override
    public Meta getKeyMeta() {
        throw new RuntimeException("Cannot look up key meta on non-map type");
    }

    @Override
    public Meta getValueMeta() {
        throw new RuntimeException("Cannot look up value meta on non-map type");
    }

    @Override
    public List<Field> getFields() {
        throw new RuntimeException("Cannot list fields on non-struct type");
    }

    @Override
    public Field getFieldByName(String fieldName) {
        throw new RuntimeException("Cannot look up fields on non-struct type");
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaBase{");
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
