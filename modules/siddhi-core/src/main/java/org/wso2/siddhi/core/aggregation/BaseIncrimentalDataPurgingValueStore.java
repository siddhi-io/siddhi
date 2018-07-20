package org.wso2.siddhi.core.aggregation;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;


public class BaseIncrimentalDataPurgingValueStore {
    private StreamEventPool streamEventPool;
    private long timestamp;
    public BaseIncrimentalDataPurgingValueStore(long timeStamp,
                                                StreamEventPool streamEventPool){
        this.streamEventPool = streamEventPool;
        this.timestamp = timeStamp;

    }

    public StateEvent createStreamEvent(Object[] values) {
        StreamEvent streamEvent = streamEventPool.borrowEvent();
        streamEvent.setTimestamp(timestamp);
        streamEvent.setOutputData(values);

        StateEvent stateEvent= new StateEvent(1,1);
        stateEvent.addEvent(0,streamEvent);
        return stateEvent;
    }
}
