package io.openmessaging.connector.api.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Meta implement for type of STRUCT.
 *
 * @author liuboyan
 */
public class MetaStruct extends Meta {

    /**
     * Structure of the meta, contains a list of {@link Field}
     */
    private List<Field> fields;
    /**
     * field name 和 field的对应map，便于快速查找
     */
    private Map<String, Field> fieldsByName;

    /**
     * Base construct.
     *
     * @param type       {@link Type}
     * @param name       Name of the meta.
     * @param version    Version of the meta.
     * @param dataSource Data source information.
     * @param parameters Possible parameters.
     */
    public MetaStruct(Type type, String name, Integer version, String dataSource, Map<String, String> parameters, List<Field> fields) {
        super(type, name, version, dataSource, parameters);
        if(type != Type.STRUCT){
            throw new RuntimeException("type should be STRUCT.");
        }
        this.fields = fields == null ? Collections.emptyList() : fields;
        this.fieldsByName = new HashMap<>(this.fields.size());
        for (Field field : this.fields) {
            fieldsByName.put(field.name(), field);
        }
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
        return this.fields;
    }

    @Override
    public Field getFieldByName(String fieldName) {
        return this.fieldsByName.get(fieldName);
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

        MetaStruct meta = (MetaStruct) o;
        return Objects.equals(this.fields, meta.fields);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.fields.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaStruct{");
        if(this.name != null){
            sb.append("name=").append(this.name).append(", ");
        }
        if (this.version != null) {
            sb.append("version=").append(this.version).append(", ");
        }
        sb.append("type=").append(this.type).append(", ");
        if(this.dataSource != null){
            sb.append("dataSource=").append(this.dataSource).append(", ");
        }
        if(this.fields!=null){
            sb.append("fields=").append(this.fields).append(", ");
        }
        sb.append("parameters=").append(this.parameters).append("}");
        return sb.toString();
    }
}
