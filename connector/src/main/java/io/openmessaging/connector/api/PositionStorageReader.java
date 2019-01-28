package io.openmessaging.connector.api;

import java.util.Collection;
import java.util.Map;

public interface PositionStorageReader {

    /**
     * Get the position for the specified partition.
     * @param partition
     * @return
     */
    <T> Map<String, ?> getPosition(Map<String, T> partition);

    /**
     * Get a set of positions for the specified partitions.
     * @param partitions
     * @return
     */
    <T> Map<Map<String, T>, Map<String, ?>> getPositions(Collection<Map<String, T>> partitions);
}
