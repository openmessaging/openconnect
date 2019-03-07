package io.openmessaging.connector.api.data;

public abstract class DataEntry {

  public DataEntry(
      Long timestamp, EntryType entryType, String queueName, Schema schema, Object[] payload) {
    this.timestamp = timestamp;
    this.entryType = entryType;
    this.queueName = queueName;
    this.schema = schema;
    this.payload = payload;
  }

  /** Timestamp of the data entry. */
  private Long timestamp;

  /** Type of the data entry. */
  private EntryType entryType;

  /** Related queueName. */
  private String queueName;

  /** Schema of the data entry. */
  private Schema schema;

  /** Payload of the data entry. */
  private Object[] payload;

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public EntryType getEntryType() {
    return entryType;
  }

  public void setEntryType(EntryType entryType) {
    this.entryType = entryType;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Object[] getPayload() {
    return payload;
  }

  public void setPayload(Object[] payload) {
    this.payload = payload;
  }
}
