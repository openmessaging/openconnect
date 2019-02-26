package io.openmessaging.connector.api.source;

import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.data.SourceDataEntry;
import java.util.Collection;

public abstract class SourceTask implements Task {

    protected SourceTaskContext context;

    /**
     * Initialize this sourceTask.
     * @param context
     */
    public void initialize(SourceTaskContext context) {
        this.context = context;
    }

    /**
     * Return a collection of message entries to send.
     * */
    public abstract Collection<SourceDataEntry> poll();
}
