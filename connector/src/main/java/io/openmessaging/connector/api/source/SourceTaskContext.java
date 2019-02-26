package io.openmessaging.connector.api.source;

import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.TaskContext;

public interface SourceTaskContext extends TaskContext {

    /**
     * Get the PositionStorageReader for this SourceTask.
     */
    PositionStorageReader offsetStorageReader();
}
