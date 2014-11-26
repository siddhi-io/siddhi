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
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.classifiers.utils.Evaluation;
import org.wso2.siddhi.classifiers.utils.InstancesUtil;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class ClassifyHtWindowProcessor extends WindowProcessor {
    private int testModeCount = -1;
    private boolean trainMode = true;
    private HoeffdingTree hoeffdingTree;
    private int attributeCount = 0;
    private List<Attribute> attributeList;
    private Instances instances;
    private List<String> outClassValues;
    private Evaluation evaluation;
    private int maxCount = 53629;
    private int totalCount = 0;
    PrintWriter writer;

    @Override
    protected void processEvent(InEvent event) {
        acquireLock();
        try {
            runClassification(event);
        } catch (Exception e) {
            throw new RuntimeException("Error building the Tree", e);
        } finally {
            releaseLock();
        }
    }

    private void runClassification(Event event) throws Exception {
        DenseInstance instance = InstancesUtil.getInstance(attributeList, event);
        instances.add(instance);
        totalCount++;
        if (instances.size() == testModeCount && trainMode) {
            hoeffdingTree.buildClassifier(instances);
            evaluation = new Evaluation(instances);
            instances = new Instances(instances.relationName(), attributeList, 0);
            instances.setClassIndex(instances.numAttributes() - 1);
            trainMode = false;
            System.gc();
        }else if (instances.size()==maxCount && trainMode){ // when the testModeCount is too high we want to build the classifier, but not change the trainMode to false
            hoeffdingTree.buildClassifier(instances);
            evaluation = new Evaluation(instances);
            instances = new Instances(instances.relationName(), attributeList, 0);
            instances.setClassIndex(instances.numAttributes() - 1);
            System.gc();
        }else if(totalCount==testModeCount && trainMode){                // this happens when testModeCount is large and classfier is built batchwise.
            hoeffdingTree.buildClassifier(instances);
            evaluation = new Evaluation(instances);
            instances = new Instances(instances.relationName(), attributeList, 0);
            instances.setClassIndex(instances.numAttributes() - 1);
            trainMode = false;
            System.gc();
        }
        else{
            if (!trainMode) {
                Enumeration<Instance> instanceEnumeration = instances.enumerateInstances();
                while (instanceEnumeration.hasMoreElements()) {
                    Instance instance1 = instanceEnumeration.nextElement();
                    double v = evaluation.evaluationForSingleInstance(hoeffdingTree, instance1);
                    writer.println((int)v);
                    if(attributeList.get(attributeList.size()-1).indexOfValue(outClassValues.get(0))== v){
                        nextProcessor.process(event);
                    }
                }
                instances = new Instances(instances.relationName(), attributeList, 0);
                instances.setClassIndex(instances.numAttributes() - 1);
            }
        }
    }

    @Override
    protected void processEvent(InListEvent listEvent) {
        acquireLock();
        try {
            Event[] events = listEvent.getEvents();
            for (int i = 0, events1Length = listEvent.getActiveEvents(); i < events1Length; i++) {
                // invoke hoeffding tree
                try {
                    runClassification(events[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
        attributeList = streamDefinition.getAttributeList();
        for (Attribute attribute : attributeList) {
            if (!Attribute.Type.NOMINAL.equals(attribute.getType())) {
                throw new SiddhiParserException("This Window can only handle nominal attributes");
            }
        }
        // during initialization we have to set the mode
        if (parameters.length == 0) {
            throw new SiddhiParserException("Should provide the minimum number of test events for tree building");
        }
        try {
            testModeCount = ((IntConstant) parameters[0]).getValue();
        } catch (ClassCastException e) {
            throw new SiddhiParserException("First input to this window should be the minimum test event count");
        }
        if (parameters.length > 1) {
            outClassValues = new ArrayList<String>();
            for (int i = 1; i < parameters.length; i++) {
                if(parameters[i] instanceof Variable) {
                    outClassValues.add(((Variable) parameters[i]).getAttributeName());
                }else if(parameters[i] instanceof IntConstant){
                    outClassValues.add(((IntConstant) parameters[i]).getValue().toString());
                }
            }
        }

        // todo we need to implement a way to load the training dataset during initialization and start the siddhi with
        // todo a properly trained tree, otherwise we have to do a check to do whether build or update the tree
        instances = new Instances(streamDefinition.getId(), streamDefinition.getAttributeList(), 0);

        instances.setClassIndex(instances.numAttributes() - 1); // this is by default and stream has to be defined accordingly

        // Test basic methods based on class index.
        System.out.println("\nClass name: " + instances.classAttribute().getName());
        System.out.println("\nClass index: " + instances.classIndex());
        System.out.println("\nClass is nominal: "
                + instances.classAttribute().isNominal());
        System.out.println("\nClass is numeric: "
                + instances.classAttribute().isNumeric());
        hoeffdingTree = new HoeffdingTree();
        try {
            writer = new PrintWriter("/tmp/evaluation.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        System.out.println(totalCount);
    }
}
