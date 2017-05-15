package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Logical not processor.
 */
public class AbsentLogicalPreStateProcessor extends LogicalPreStateProcessor implements SchedulingProcessor {

    private Scheduler scheduler;
    private List<StateEvent> arrivedEventsList = new LinkedList<>();
    private long timeout;
    private boolean isFirstInPattern = false;
    private boolean noPresentBeforeInPattern = false;

    public AbsentLogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType, List<Map
            .Entry<Long, Set<Integer>>> withinStates) {
        super(type, stateType, Collections.EMPTY_LIST);
        if (withinStates.size() > 0) {
            timeout = withinStates.get(0).getKey();
        }
    }

    @Override
    public void addState(StateEvent stateEvent) {
        super.addState(stateEvent);
        if (logicalType == LogicalStateElement.Type.OR) {
            synchronized (this) {
                arrivedEventsList.add(stateEvent);
            }
            scheduler.notifyAt(stateEvent.getTimestamp() + timeout);
        }
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {

        if (logicalType == LogicalStateElement.Type.OR) {


            // Called by the scheduler
//            if (isAbsentPartner()) {
//                while (complexEventChunk.hasNext()) {
//                    ComplexEvent newEvent = complexEventChunk.next();
//                    if (newEvent.getType() == ComplexEvent.Type.TIMER) {
//                        long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
//                        for (Iterator<StateEvent> iterator = pendingStateEventList.iterator(); iterator.hasNext(); ) {
//                            if (currentTime >= iterator.next().getTimestamp() + getAbsentPartnerTimeout()) {
//                                iterator.remove();
//                            }
//                        }
//                    }
//                }
//            } else {
            while (complexEventChunk.hasNext()) {

                ComplexEvent newEvent = complexEventChunk.next();
                if (newEvent.getType() == ComplexEvent.Type.TIMER) {
                    long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                    ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<StateEvent>(false);

                    // Synchronize with processAndReturn method
                    synchronized (this) {
                        Iterator<StateEvent> iterator = arrivedEventsList.iterator();
                        while (iterator.hasNext()) {
                            StateEvent event = iterator.next();
                            if (currentTime >= event.getTimestamp() + timeout) {
                                iterator.remove();
                                retEventChunk.add(event);
                            }
                        }
                    }

                    if (this.getThisStatePostProcessor().getNextProcessor() != null) {
                        // Next processor is StreamStatePostProcessor
                        QuerySelector querySelector = (QuerySelector) this.getThisStatePostProcessor()
                                .getNextProcessor();
                        while (retEventChunk.hasNext()) {
                            StateEvent stateEvent = retEventChunk.next();
                            retEventChunk.remove();
                            querySelector.process(new ComplexEventChunk<StateEvent>(stateEvent, stateEvent, false));
                        }
                    } else if (this.getThisStatePostProcessor().getNextStatePerProcessor() != null) {
                        // No next StreamStatePostProcessor, if this is part of pattern
                        while (retEventChunk.hasNext()) {
                            StateEvent stateEvent = retEventChunk.next();
                            retEventChunk.remove();
                            ((StreamPreStateProcessor) this.getThisStatePostProcessor().getNextStatePerProcessor())
                                    .addAbsent(stateEvent);

                        }
                    }
                }
            }
//            }
        } else {
            super.process(complexEventChunk);
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        if (logicalType == LogicalStateElement.Type.OR) {
            StateEvent firstEvent = event.getFirst();
            if (firstEvent != null) {
                // Synchronize with process method
                synchronized (this) {
                    arrivedEventsList.remove(firstEvent);
                }
                event = new ComplexEventChunk<StateEvent>(false);
            }
        }
        return event;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }
}
