/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.connector.api.header;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.openmessaging.connector.api.data.Date;
import io.openmessaging.connector.api.data.Decimal;
import io.openmessaging.connector.api.data.Meta;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.Time;
import io.openmessaging.connector.api.data.Timestamp;
import io.openmessaging.connector.api.data.Type;

/**
 * A basic {@link Headers} implementation.
 */
public class DataHeaders implements Headers {

    private static final int EMPTY_HASH = Objects.hash(new LinkedList<>());

    /**
     * An immutable and therefore sharable empty iterator.
     */
    private static final Iterator<Header> EMPTY_ITERATOR = new Iterator<Header>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Header next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new IllegalStateException();
        }
    };

    /**
     * This field is set lazily, but once set to a list it is never set back to null
     */
    private Map<String, Header> headers;

    public DataHeaders() {
    }

    public DataHeaders(Iterable<Header> original) {
        if (original == null) {
            return;
        }
        if (original instanceof DataHeaders) {
            DataHeaders originalHeaders = (DataHeaders) original;
            if (!originalHeaders.isEmpty()) {
                headers = new LinkedHashMap<>(originalHeaders.headers);
            }
        } else {
            headers = new LinkedHashMap<>();
            for (Header header : original) {
                headers.put(header.key(), header);
            }
        }
    }

    @Override
    public int size() {
        return headers == null ? 0 : headers.size();
    }

    @Override
    public boolean isEmpty() {
        return headers == null ? true : headers.isEmpty();
    }

    @Override
    public Headers clear() {
        if (headers != null) {
            headers.clear();
        }
        return this;
    }

    @Override
    public Headers add(Header header) {
        Objects.requireNonNull(header, "Unable to add a null header.");
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        headers.put(header.key(), header);
        return this;
    }

    protected Headers addWithoutValidating(String key, Object value, Meta meta) {
        return add(new DataHeader(key, meta, value));
    }

    @Override
    public Headers add(String key, Meta meta, Object value) {
        checkMetaMatches(meta, value);
        return add(new DataHeader(key, meta, value));
    }

    @Override
    public Headers addString(String key, String value) {
        return addWithoutValidating(key, value, Meta.STRING_META);
    }

    @Override
    public Headers addBytes(String key, byte[] value) {
        return addWithoutValidating(key, value, Meta.BYTES_META);
    }

    @Override
    public Headers addBoolean(String key, boolean value) {
        return addWithoutValidating(key, value, Meta.BOOLEAN_META);
    }

    @Override
    public Headers addByte(String key, byte value) {
        return addWithoutValidating(key, value, Meta.INT8_META);
    }

    @Override
    public Headers addShort(String key, short value) {
        return addWithoutValidating(key, value, Meta.INT16_META);
    }

    @Override
    public Headers addInt(String key, int value) {
        return addWithoutValidating(key, value, Meta.INT32_META);
    }

    @Override
    public Headers addLong(String key, long value) {
        return addWithoutValidating(key, value, Meta.INT64_META);
    }

    @Override
    public Headers addFloat(String key, float value) {
        return addWithoutValidating(key, value, Meta.FLOAT32_META);
    }

    @Override
    public Headers addDouble(String key, double value) {
        return addWithoutValidating(key, value, Meta.FLOAT64_META);
    }

    @Override
    public Headers addList(String key, List<?> value, Meta meta) {
        if (value == null) {
            return add(key, null, null);
        }
        checkMetaType(meta, Type.ARRAY);
        return addWithoutValidating(key, value, meta);
    }

    @Override
    public Headers addMap(String key, Map<?, ?> value, Meta meta) {
        if (value == null) {
            return add(key, null, null);
        }
        checkMetaType(meta, Type.MAP);
        return addWithoutValidating(key, value, meta);
    }

    @Override
    public Headers addStruct(String key, Struct value) {
        if (value == null) {
            return add(key, null, null);
        }
        checkMetaType(value.meta(), Type.STRUCT);
        return addWithoutValidating(key, value, value.meta());
    }

    @Override
    public Headers addDecimal(String key, BigDecimal value) {
        if (value == null) {
            return add(key, null, null);
        }
        // Check that this is a decimal ...
        Meta meta = Decimal.meta(value.scale());
        Decimal.fromLogical(meta, value);
        return addWithoutValidating(key, value, meta);
    }

    @Override
    public Headers addDate(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a date ...
            Date.fromLogical(Date.META, value);
        }
        return addWithoutValidating(key, value, Date.META);
    }

    @Override
    public Headers addTime(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a time ...
            Time.fromLogical(Time.META, value);
        }
        return addWithoutValidating(key, value, Time.META);
    }

    @Override
    public Headers addTimestamp(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a timestamp ...
            Timestamp.fromLogical(Timestamp.META, value);
        }
        return addWithoutValidating(key, value, Timestamp.META);
    }

    @Override
    public Header findHeader(String key) {
        return new FilterByKeyIterator(iterator(), key).makeNext();
    }

    @Override
    public Map<String, Header> toMap(){
        return new HashMap<>(this.headers);
    }

    @Override
    public Iterator<Header> iterator() {
        if (headers != null) {
            return headers.values().iterator();
        }
        return EMPTY_ITERATOR;
    }

    @Override
    public Headers remove(String key) {
        checkKey(key);
        if (!isEmpty()) {
            Iterator<Header> iterator = iterator();
            while (iterator.hasNext()) {
                if (iterator.next().key().equals(key)) {
                    iterator.remove();
                }
            }
        }
        return this;
    }
    @Override
    public int hashCode() {
        return isEmpty() ? EMPTY_HASH : Objects.hash(headers);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Headers) {
            Headers that = (Headers) obj;
            Iterator<Header> thisIter = this.iterator();
            Iterator<Header> thatIter = that.iterator();
            while (thisIter.hasNext() && thatIter.hasNext()) {
                if (!Objects.equals(thisIter.next(), thatIter.next())) {
                    return false;
                }
            }
            return !thisIter.hasNext() && !thatIter.hasNext();
        }
        return false;
    }

    @Override
    public String toString() {
        return "DataHeaders(headers=" + (headers != null ? headers : "") + ")";
    }

    @Override
    public DataHeaders duplicate() {
        return new DataHeaders(this);
    }

    /**
     * Check that the key is not null
     *
     * @param key the key; may not be null
     * @throws NullPointerException if the supplied key is null
     */
    private void checkKey(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
    }

    /**
     * Check the {@link Type() meta's type} matches the specified type.
     *
     * @param meta the meta; never null
     * @param type   the expected type
     * @throws RuntimeException if the meta's type does not match the expected type
     */
    private void checkMetaType(Meta meta, Type type) {
        if (meta.getType() != type) {
            throw new RuntimeException("Expecting " + type + " but instead found " + meta.getType());
        }
    }

    /**
     * Check that the value and its meta are compatible.
     *
     * @param meta
     * @param value the meta and value pair
     * @throws RuntimeException if the meta is not compatible with the value
     */
    private void checkMetaMatches(Meta meta, Object value) {
        if (meta != null && value != null) {
            switch (meta.getType()) {
                case BYTES:
                    if (value instanceof ByteBuffer) {
                        return;
                    }
                    if (value instanceof byte[]) {
                        return;
                    }
                    if (value instanceof BigDecimal && Decimal.LOGICAL_NAME.equals(meta.getName())) {
                        return;
                    }
                    break;
                case STRING:
                    if (value instanceof String) {
                        return;
                    }
                    break;
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return;
                    }
                    break;
                case INT8:
                    if (value instanceof Byte) {
                        return;
                    }
                    break;
                case INT16:
                    if (value instanceof Short) {
                        return;
                    }
                    break;
                case INT32:
                    if (value instanceof Integer) {
                        return;
                    }
                    if (value instanceof java.util.Date && Date.LOGICAL_NAME.equals(meta.getName())) {
                        return;
                    }
                    if (value instanceof java.util.Date && Time.LOGICAL_NAME.equals(meta.getName())) {
                        return;
                    }
                    break;
                case INT64:
                    if (value instanceof Long) {
                        return;
                    }
                    if (value instanceof java.util.Date && Timestamp.LOGICAL_NAME.equals(meta.getName())) {
                        return;
                    }
                    break;
                case FLOAT32:
                    if (value instanceof Float) {
                        return;
                    }
                    break;
                case FLOAT64:
                    if (value instanceof Double) {
                        return;
                    }
                    break;
                case ARRAY:
                    if (value instanceof List) {
                        return;
                    }
                    break;
                case MAP:
                    if (value instanceof Map) {
                        return;
                    }
                    break;
                case STRUCT:
                    if (value instanceof Struct) {
                        return;
                    }
                    break;
            }
            throw new RuntimeException("The value " + value + " is not compatible with the meta " + meta);
        }
    }

    private static final class FilterByKeyIterator implements Iterator<Header> {
        private enum State {
            READY, NOT_READY, DONE, FAILED
        }

        private State state = State.NOT_READY;
        private Header next;


        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }

        protected Header makeNext() {
            while (original.hasNext()) {
                Header header = original.next();
                if (!header.key().equals(key)) {
                    continue;
                }
                return header;
            }
            return this.allDone();
        }

        @Override
        public boolean hasNext() {
            switch (state) {
                case FAILED:
                    throw new IllegalStateException("Iterator is in failed state");
                case DONE:
                    return false;
                case READY:
                    return true;
                default:
                    return maybeComputeNext();
            }
        }

        @Override
        public Header next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            state = State.NOT_READY;
            if (next == null) {
                throw new IllegalStateException("Expected item but none found.");
            }
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removal not supported");
        }




        public Header peek() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next;
        }

        protected Header allDone() {
            state = State.DONE;
            return null;
        }


        private Boolean maybeComputeNext() {
            state = State.FAILED;
            next = makeNext();
            if (state == State.DONE) {
                return false;
            } else {
                state = State.READY;
                return true;
            }
        }
    }
}
