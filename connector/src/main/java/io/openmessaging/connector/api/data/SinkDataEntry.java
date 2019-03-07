package io.openmessaging.connector.api.data;

public class SinkDataEntry extends DataEntry {

  public SinkDataEntry(
      Long queueOffset,
      Long timestamp,
      EntryType entryType,
      String queueName,
      Schema schema,
      Object[] payload) {
    super(timestamp, entryType, queueName, schema, payload);
    this.queueOffset = queueOffset;
  }

  /** offset in the queue. */
  private Long queueOffset;

  public Long getQueueOffset() {
    return queueOffset;
  }

  public void setQueueOffset(Long queueOffset) {
    this.queueOffset = queueOffset;
  }
}
