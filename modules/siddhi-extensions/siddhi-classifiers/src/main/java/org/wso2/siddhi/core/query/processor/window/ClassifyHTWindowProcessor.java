/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.core.query.processor.window;

import org.wso2.siddhi.classifiers.trees.ht.DenseInstance;
import org.wso2.siddhi.classifiers.trees.ht.HoeffdingTree;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.core.query.processor.window.WindowProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.Iterator;
import java.util.List;

public class ClassifyHtWindowProcessor extends WindowProcessor {
    int testModeCount = -1;
    boolean trainMode = true;
    HoeffdingTree hoeffdingTree;

    @Override
    protected void processEvent(InEvent event) {

    }

    @Override
    protected void processEvent(InListEvent listEvent) {
        acquireLock();
        try {
            Event[] events = listEvent.getEvents();
            for (int i = 0, events1Length = listEvent.getActiveEvents(); i < events1Length; i++) {
                // invoke hoeffding tree


            }
        } finally {
            releaseLock();
        }
    }

    @Override
    public Iterator<StreamEvent> iterator() {
        return null;
    }

    @Override
    public Iterator<StreamEvent> iterator(String predicate) {
        return null;
    }

    @Override
    protected Object[] currentState() {
        return new Object[0];
    }

    @Override
    protected void restoreState(Object[] data) {

    }

    @Override
    protected void init(Expression[] parameters, QueryPostProcessingElement nextProcessor, AbstractDefinition streamDefinition, String elementId, boolean async, SiddhiContext siddhiContext) {
        // first we check the stream definition, currently we do not support numerica attributes,
        // we first check the stream only have nominal attributes
        List<Attribute> attributeList = streamDefinition.getAttributeList();
        for(Attribute attribute: attributeList){
            if(!Attribute.Type.NOMINAL.equals(attribute.getType())) {
                throw new SiddhiParserException("This Window can only handle nominal attributes");
            }
        }
        // during initialization we have to set the mode
        if(parameters.length==0){
            throw new SiddhiParserException("Should provide the minimum number of test events for tree building");
        }
        try {
            testModeCount = ((IntConstant) parameters[0]).getValue();
        }catch (ClassCastException e){
            throw new SiddhiParserException("First input to this window should be the minimum test event count");
        }

        // todo we need to implement a way to load the training dataset during initialization and start the siddhi with
        // todo a properly trained tree, otherwise we have to do a check to do whether build or update the tree
        Instances instances = new Instances(streamDefinition.getId(), streamDefinition.getAttributeList(), 10);
        hoeffdingTree = new HoeffdingTree();
        try {
            hoeffdingTree.buildClassifier(instances);
        } catch (Exception e) {
            throw new RuntimeException("Error building the Tree");
        }
    }

    @Override
    public void destroy() {

    }
}
