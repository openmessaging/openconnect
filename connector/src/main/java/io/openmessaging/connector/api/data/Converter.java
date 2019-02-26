package io.openmessaging.connector.api.data;

public interface Converter<T> {

    /**
     * Method to serialize the payload of DataEntry.
     * @param object
     * @return
     */
    byte[] objectToByte(T object);

    /**
     * Method to deserialize the payload of DataEntry.
     * @param bytes
     * @return
     */
    T byteToObject(byte[] bytes);
}
