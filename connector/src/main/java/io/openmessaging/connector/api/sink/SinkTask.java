package io.openmessaging.connector.api.sink;

import io.openmessaging.Message;
import io.openmessaging.connector.api.Task;
import java.util.Collection;

public abstract class SinkTask implements Task {

    protected SinkTaskContext context;

    /**
     * Initialize this sinkTask.
     * @param context
     */
    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Put the data entries to the sink.
     * */
    abstract void put(Collection<Message> message);
}
