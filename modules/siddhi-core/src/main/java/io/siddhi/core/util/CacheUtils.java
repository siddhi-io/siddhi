package io.siddhi.core.util;

import io.siddhi.core.event.stream.StreamEvent;

/**
 * class containing utils related to store table cache
 */
public class CacheUtils {
    public static int findEventChunkSize(StreamEvent streamEvent) {
        int chunkSize = 1;
        StreamEvent streamEventCopy = streamEvent;
        while (streamEvent.hasNext()) {
            chunkSize = chunkSize + 1;
            streamEventCopy = streamEventCopy.getNext();
        }
        return chunkSize;
    }
}
