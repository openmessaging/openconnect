package io.openmessaging.connector.api.data;

public interface Converter<T> {

  /**
   * Method to serialize the payload of DataEntry.
   *
   * @param object this object needs to be converted to byte[].
   * @return converted value.
   */
  byte[] objectToByte(T object);

  /**
   * Method to deserialize the payload of DataEntry.
   *
   * @param bytes this bytes needs to be converted to the required class.
   * @return converted value.
   */
  T byteToObject(byte[] bytes);
}
