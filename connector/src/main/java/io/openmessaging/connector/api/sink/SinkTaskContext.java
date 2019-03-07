package io.openmessaging.connector.api.sink;

import io.openmessaging.connector.api.TaskContext;
import java.util.List;
import java.util.Map;

public interface SinkTaskContext extends TaskContext {

  /**
   * Reset the consumer offset for the given queue.
   *
   * @param queueName the queuename to reset offset.
   * @param offset the offset to reset to.
   */
  void resetOffset(String queueName, Long offset);

  /**
   * Reset the consumer offsets for the given queue.
   *
   * @param offsets map of offsets for queuename.
   */
  void resetOffset(Map<String, Long> offsets);

  /**
   * Pause consumption of messages from the specified TopicPartitions.
   *
   * @param queueName the queuename to reset offset.
   */
  void pause(List<String> queueName);

  /**
   * Resume consumption of messages from previously paused TopicPartitions.
   *
   * @param queueName the queuename to reset offset.
   */
  void resume(List<String> queueName);
}
