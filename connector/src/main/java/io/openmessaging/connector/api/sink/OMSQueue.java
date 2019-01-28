package io.openmessaging.connector.api.sink;

/**
 * Define the target queue of a standard message.
 */
public class OMSQueue {

    private String queue;
    private Integer partition;

    public OMSQueue(){}

    public OMSQueue(String queue, Integer partition){
        this.queue = queue;
        this.partition = partition;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }
}
