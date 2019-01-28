package io.openmessaging.connector.api.sink;

import io.openmessaging.connector.api.TaskContext;
import java.util.List;
import java.util.Map;

public interface SinkTaskContext extends TaskContext {

    /**
     * Reset the consumer offset for the given queue.
     * @param OMSQueue
     * @param offset
     */
    void resetOffset(OMSQueue OMSQueue, Long offset);

    /**
     * Reset the consumer offsets for the given queue.
     * @param offsets
     */
    void resetOffset(Map<OMSQueue, Long> offsets);

    /**
     * Pause consumption of messages from the specified TopicPartitions.
     * @param OMSQueues
     */
    void pause(List<OMSQueue> OMSQueues);

    /**
     * Resume consumption of messages from previously paused TopicPartitions.
     * @param OMSQueues
     */
    void resume(List<OMSQueue> OMSQueues);
}
