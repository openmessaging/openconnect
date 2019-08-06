package io.openmessaging.connector.api.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MetaBuilder implement for type of STRUCT.
 *
 * @author liuboyan
 */
public class MetaStructBuilder extends MetaBuilder {
    private Map<String, Field> fields;

    public MetaStructBuilder(Type type) {
        super(type);
        this.fields = new LinkedHashMap<>();
    }

    @Override
    public MetaBuilder field(String fieldName, Meta fieldMeta) {
        if (null == fieldName || fieldName.isEmpty()) {
            throw new RuntimeException("fieldName cannot be null.");
        }
        if (null == fieldMeta) {
            throw new RuntimeException("fieldMeta for field " + fieldName + " cannot be null.");
        }
        int fieldIndex = this.fields.size();
        if (this.fields.containsKey(fieldName)) {
            throw new RuntimeException("Cannot create field because of field name duplication " + fieldName);
        }
        this.fields.put(fieldName, new Field(fieldIndex, fieldName, fieldMeta));
        return this;
    }

    @Override
    public MetaBuilder field(Field field) {
        return this.field(field.name(), field.meta());
    }

    @Override
    public Meta keyMeta() {
        return null;
    }

    @Override
    public Meta valueMeta() {
        return null;
    }

    @Override
    public List<Field> fields() {
        return new ArrayList<>(this.fields.values());
    }

    @Override
    public Field getFieldByName(String fieldName) {
        return fields.get(fieldName);
    }

    @Override
    public Meta build() {
        return new MetaStruct(this.type(),
            this.name(),
            this.version(),
            this.dataSource(),
            this.parameters() == null ? null : Collections.unmodifiableMap(this.parameters()),
            fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields.values())));
    }
}
