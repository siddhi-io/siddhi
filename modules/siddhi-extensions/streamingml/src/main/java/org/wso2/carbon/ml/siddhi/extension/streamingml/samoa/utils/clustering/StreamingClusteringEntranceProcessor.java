/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.clustering;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.learners.clusterers.ClusteringContentEvent;
import org.apache.samoa.moa.core.DataPoint;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.SourceProcessor;

public class StreamingClusteringEntranceProcessor extends SourceProcessor {

    @Override
    public ContentEvent nextEvent() {
        if (isFinished()) {
            // send ending event
            ClusteringContentEvent contentEvent = new ClusteringContentEvent(-1, firstInstance);
            contentEvent.setLast(true);
            return contentEvent;
        } else {
            DataPoint nextDataPoint = new DataPoint(nextInstance(), numberOfInstancesSent);
            numberOfInstancesSent++;
            ClusteringContentEvent contentEvent = new ClusteringContentEvent(numberOfInstancesSent,
                    nextDataPoint);
            return contentEvent;
        }
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingClusteringEntranceProcessor newProcessor =
                new StreamingClusteringEntranceProcessor();
        StreamingClusteringEntranceProcessor originProcessor =
                (StreamingClusteringEntranceProcessor) p;
        if (originProcessor.getStreamSource() != null) {
            newProcessor.setStreamSource(originProcessor.getStreamSource().getStream());
        }
        return newProcessor;
    }
}
