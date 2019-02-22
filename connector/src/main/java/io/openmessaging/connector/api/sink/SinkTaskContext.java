package io.openmessaging.connector.api.sink;

import io.openmessaging.connector.api.TaskContext;
import java.util.List;
import java.util.Map;

public interface SinkTaskContext extends TaskContext {

    /**
     * Reset the consumer offset for the given queue.
     * @param queueName
     * @param offset
     */
    void resetOffset(String queueName, Long offset);

    /**
     * Reset the consumer offsets for the given queue.
     * @param offsets
     */
    void resetOffset(Map<String, Long> offsets);

    /**
     * Pause consumption of messages from the specified TopicPartitions.
     * @param queueName
     */
    void pause(List<String> queueName);

    /**
     * Resume consumption of messages from previously paused TopicPartitions.
     * @param queueName
     */
    void resume(List<String> queueName);
}
