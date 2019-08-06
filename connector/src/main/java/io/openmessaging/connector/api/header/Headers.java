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
import java.util.List;
import java.util.Map;

import io.openmessaging.connector.api.data.Meta;
import io.openmessaging.connector.api.data.Struct;

/**
 * A mutable ordered collection of {@link Header} objects.
 * Note that multiple headers shouldn't have the same {@link Header#key() key}.
 */
public interface Headers extends Iterable<Header> {

    /**
     * Get the number of headers in this object.
     *
     * @return the number of headers; never negative
     */
    int size();

    /**
     * Determine whether this object has no headers.
     *
     * @return true if there are no headers, or false if there is at least one header
     */
    boolean isEmpty();

    /**
     * Get the collection of {@link Header} objects whose {@link Header#key() keys} all match the specified key.
     *
     * @param key the key; may not be null
     * @return the iterator over headers with the specified key; may be null if there are no headers with the
     * specified key
     */
    Header findHeader(String key);

    /**
     * Get the map of {@link Header} objects.
     *
     * @return the map of headers
     */
    Map<String, Header> toMap();

    /**
     * Add the given {@link Header} to this collection.
     *
     * @param header the header; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers add(Header header);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @param meta  the meta for the header's value; may not be null if the value is not null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers add(String key, Meta meta, Object value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addString(String key, String value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addBoolean(String key, boolean value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addByte(String key, byte value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addShort(String key, short value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addInt(String key, int value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addLong(String key, long value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addFloat(String key, float value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDouble(String key, double value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addBytes(String key, byte[] value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @param meta  the meta describing the list value; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws Exception if the header's value is invalid
     */
    Headers addList(String key, List<?> value, Meta meta);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @param meta  the meta describing the map value; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws Exception if the header's value is invalid
     */
    Headers addMap(String key, Map<?, ?> value, Meta meta);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws Exception if the header's value is invalid
     */
    Headers addStruct(String key, Struct value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDecimal(String key, BigDecimal value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDate(String key, java.util.Date value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addTime(String key, java.util.Date value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addTimestamp(String key, java.util.Date value);

    /**
     * Removes all {@link Header} objects whose {@link Header#key() key} matches the specified key.
     *
     * @param key the key; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers remove(String key);

    /**
     * Removes all headers from this object.
     *
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers clear();

    /**
     * Create a copy of this {@link Headers} object. The new copy will contain all of the same {@link Header} objects as
     * this object.
     *
     * @return the copy; never null
     */
    Headers duplicate();

}
