/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.error.handler.util;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.model.ReplayableTableRecord;
import io.siddhi.core.util.error.handler.store.ErrorStore;

import java.util.List;

/**
 * Acts as the static collector which is called to send erroneous events during error occurrences in Siddhi.
 */
public class ErrorStoreHelper {

    private ErrorStoreHelper() {
    }

    /**
     * Sends an erroneous event to the error store in order to save that.
     *
     * @param errorStore     Error store object.
     * @param occurrence     Occurrence point of the error.
     * @param siddhiAppName  Siddhi app name.
     * @param erroneousEvent The event which was collected due to an error.
     * @param streamName     The stream from which, error was collected.
     */
    public static void storeErroneousEvent(ErrorStore errorStore, ErrorOccurrence occurrence, String siddhiAppName,
                                           Object erroneousEvent, String streamName) {
        if (errorStore != null && erroneousEvent != null) {
            switch (getErrorType(occurrence)) {
                case MAPPING:
                    if (erroneousEvent instanceof List) {
                        errorStore.saveMappingError(siddhiAppName, (List<ErroneousEvent>) erroneousEvent,
                                streamName);
                    }
                    break;
                case TRANSPORT:
                    errorStore.saveTransportError(siddhiAppName, (ErroneousEvent) erroneousEvent,
                            getErroneousEventType(((ErroneousEvent) erroneousEvent).getEvent()), streamName,
                            occurrence);
                    break;
            }
        }
    }

    private static ErrorType getErrorType(ErrorOccurrence errorOccurrence) {
        if (errorOccurrence == ErrorOccurrence.BEFORE_SOURCE_MAPPING) {
            return ErrorType.MAPPING;
        } else {
            return ErrorType.TRANSPORT;
        }
    }

    private static ErroneousEventType getErroneousEventType(Object event) {
        if (event instanceof ReplayableTableRecord) {
            return ErroneousEventType.REPLAYABLE_TABLE_RECORD;
        } else if (event instanceof ComplexEvent) {
            return ErroneousEventType.COMPLEX_EVENT;
        } else if (event instanceof Event) {
            return ErroneousEventType.EVENT;
        } else if (event instanceof Event[]) {
            return ErroneousEventType.EVENT_ARRAY;
        } else if (event instanceof List) {
            return ErroneousEventType.EVENT_LIST;
        } else {
            return ErroneousEventType.PAYLOAD_STRING;
        }
    }
}
