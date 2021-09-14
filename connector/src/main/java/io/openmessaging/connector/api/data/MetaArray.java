package io.openmessaging.connector.api.data;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Meta implement for type of ARRAY.
 *
 * @author liuboyan
 */
public class MetaArray extends Meta {
    private final Meta valueMeta;

    /**
     * Base construct.
     *
     * @param type       {@link Type}
     * @param version    Version of the meta.
     * @param name       Name of the meta.
     * @param dataSource Data source information.
     * @param parameters Possible parameters.
     */
    public MetaArray(Type type, String name, Integer version, String dataSource, Map<String, String> parameters, Meta valueMeta) {
        super(type, name, version, dataSource, parameters);
        if (type != Type.ARRAY) {
            throw new RuntimeException("type should be ARRAY.");
        }
        this.valueMeta = valueMeta;
    }

    @Override
    public Meta getKeyMeta() {
        throw new RuntimeException("Cannot look up key meta on non-map type");
    }

    @Override
    public Meta getValueMeta() {
        return this.valueMeta;
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MetaArray meta = (MetaArray)o;
        return Objects.equals(this.valueMeta, meta.valueMeta);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.valueMeta.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaArray{");
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
        if (this.valueMeta != null) {
            sb.append("valueMeta=").append(this.valueMeta).append(", ");
        }
        sb.append("parameters=").append(this.parameters).append("}");
        return sb.toString();
    }
}
