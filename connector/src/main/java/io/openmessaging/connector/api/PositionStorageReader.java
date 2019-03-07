package io.openmessaging.connector.api;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public interface PositionStorageReader {

    /**
     * Get the position for the specified partition.
     * @param partition
     * @return
     */
    ByteBuffer getPosition(ByteBuffer partition);

    /**
     * Get a set of positions for the specified partitions.
     * @param partitions
     * @return
     */
    Map<ByteBuffer,ByteBuffer> getPositions(Collection<ByteBuffer> partitions);
}
