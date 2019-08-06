package io.openmessaging.connector.api.data;

import java.util.Collections;
import java.util.List;

/**
 * MetaBuilder implement for type of ARRAY.
 *
 * @author liuboyan
 */
public class MetaArrayBuilder extends MetaBuilder {
    private Meta valueMeta;

    public MetaArrayBuilder(Type type, Meta valueMeta) {
        super(type);
        this.valueMeta = valueMeta;
    }

    @Override
    public MetaBuilder field(String fieldName, Meta fieldMeta) {
        throw new RuntimeException("Cannot create fields on type " + this.type());
    }

    @Override
    public MetaBuilder field(Field field) {
        throw new RuntimeException("Cannot create fields on type " + this.type());
    }

    @Override
    public Meta keyMeta() {
        return null;
    }

    @Override
    public Meta valueMeta() {
        return this.valueMeta;
    }

    @Override
    public List<Field> fields() {
        throw new RuntimeException("Cannot list fields on non-struct type");
    }

    @Override
    public Field getFieldByName(String fieldName) {
        throw new RuntimeException("Cannot look up fields on non-struct type");
    }

    @Override
    public Meta build() {
        return new MetaArray(this.type(), this.name(), this.version(),
            this.dataSource(), this.parameters() == null ? null : Collections.unmodifiableMap(this.parameters()),
            this.valueMeta);
    }
}
