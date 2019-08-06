package io.openmessaging.connector.api.data;

import java.util.Collections;
import java.util.List;

/**
 * MetaBuilder implement for base types.
 * {@link Type}.INT8 {@link Type}.INT16 {@link Type}.INT32 {@link Type}.INT64
 * {@link Type}.BIG_INTEGER {@link Type}.FLOAT32 {@link Type}.FLOAT64 {@link Type}.BOOLEAN
 * {@link Type}.STRING {@link Type}.BYTES {@link Type}.DATETIME
 *
 * @author liuboyan
 */
public class MetaBaseBuilder extends MetaBuilder {
    public MetaBaseBuilder(Type type) {
        super(type);
    }

    @Override
    public MetaBuilder field(String fieldName, Meta fieldMeta) {
        return this;
    }

    @Override
    public MetaBuilder field(Field field) {
        return this;
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
        return null;
    }

    @Override
    public Field getFieldByName(String fieldName) {
        return null;
    }

    @Override
    public Meta build() {
        return new MetaBase(this.type(), this.name(), this.version(),
            this.dataSource(), this.parameters() == null ? null : Collections.unmodifiableMap(this.parameters()));
    }
}
