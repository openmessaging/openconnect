package io.openmessaging.connector.api;

import java.util.Collection;
import java.util.Map;

public interface PositionStorageReader {

    /**
     * Get the position for the specified partition.
     * @param partition
     * @return
     */
    byte[] getPosition(byte[] partition);

    /**
     * Get a set of positions for the specified partitions.
     * @param partitions
     * @return
     */
    Map<byte[], byte[]> getPositions(Collection<byte[]> partitions);
}
