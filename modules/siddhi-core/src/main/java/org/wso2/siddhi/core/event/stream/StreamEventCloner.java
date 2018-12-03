package org.wso2.siddhi.core.event.stream;

/**
 * Cloner interface to be implemented when creating {@link org.wso2.siddhi.core.partition.PartitionRuntime}
 */
public interface StreamEventCloner {
    StreamEvent copyStreamEvent(StreamEvent streamEvent);
}
