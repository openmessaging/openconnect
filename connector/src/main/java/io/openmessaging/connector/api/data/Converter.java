/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.connector.api.data;

/**
 * A converter used to convert between {@link ConnectRecord} and byte[].
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public interface Converter<T> {

    /**
     * Method to serialize the {@link ConnectRecord}.
     *
     * @param object this object needs to be converted to byte[].
     * @return converted value.
     */
    byte[] objectToByte(T object);

    /**
     * Method to deserialize the {@link ConnectRecord}.
     *
     * @param bytes this bytes needs to be converted to the required class.
     * @return converted value.
     */
    T byteToObject(byte[] bytes);
}
