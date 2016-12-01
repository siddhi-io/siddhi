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

package org.wso2.carbon.ml.siddhi.extension.streamingml;

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification.StreamingClassification;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class StreamingClassificationExtension extends StreamProcessor implements SchedulingProcessor {

    private int numberOfAttributes;
    private int numberOfClasses;
    private int numberOfNominals;
    private String nominalAttributesValues;
    private int maxInstance;
    private int batchSize;
    private int parallelism;
    private int numberModelsBagging;
    private int parameterPosition;
    private int numberOfNumerics;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;

    private StreamingClassification streamingClassification;

    private List<String> classes = new ArrayList<String>();           //values of class attribute
    private List<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();//values of other nominal attributes

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        maxInstance = Integer.MAX_VALUE;
        batchSize = 1000;
        parallelism = 1;
        numberModelsBagging = 0;

        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                numberOfAttributes = ((Integer) attributeExpressionExecutors[0].execute(null));
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                        " first argument, required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[0].getReturnType().toString());
            }
        } else {
            throw new ExecutionPlanValidationException("Parameter count must be a constant");
        }

        int parameterWidth = attributeExpressionLength - numberOfAttributes;

        if (parameterWidth > 3) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    numberOfClasses = ((Integer) attributeExpressionExecutors[1].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the second argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Number of classes count must be" +
                        " a constant");
            }

            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    numberOfNominals = ((Integer) attributeExpressionExecutors[2].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the third argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[2].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Nominal attribute count must be a" +
                        " constant");
            }

            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.STRING) {
                    nominalAttributesValues = ((String) attributeExpressionExecutors[3].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the fourth argument,required " + Attribute.Type.STRING + " but found "
                            + attributeExpressionExecutors[3].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Nominal attributes values" +
                        " must be a constant");
            }

            String[] temp = nominalAttributesValues.split(",");
            if (temp.length != numberOfNominals && numberOfNominals != 0) {
                throw new ExecutionPlanValidationException("Number of nominal attributes and" +
                        " entered nominal attribute values are different. required " +
                        numberOfNominals + "nominal attributes but found " + temp.length);
            }
        } else {
            throw new ExecutionPlanValidationException("Invalid number of parameters. "
                    + parameterWidth + "parameters found, but required atleast 4 configuration " +
                    "parameters. streamingClassificationSamoa(parameterCount, numberOfClasses, " +
                    "numberOfNominals, valuesOfNominals,attribute_set) ");
        }
        if (parameterWidth > 4) {
            if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                    batchSize = ((Integer) attributeExpressionExecutors[4].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the fifth argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[4].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Batch size values must be a constant");
            }
        }

        if (parameterWidth > 5) {
            if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.INT) {
                    parallelism = ((Integer) attributeExpressionExecutors[5].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the sixth argument, required " + Attribute.Type.INT + " but found "
                            + attributeExpressionExecutors[5].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Parallelism value must be a constant");
            }
        }

        if (parameterWidth > 6) {
            if (attributeExpressionExecutors[6] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.INT) {
                    numberModelsBagging = ((Integer) attributeExpressionExecutors[6].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the seventh argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[6].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Number of models(For bagging)" +
                        " must be a constant");
            }
        }

        if (parameterWidth > 7) {
            if (attributeExpressionExecutors[7] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.INT) {
                    maxInstance = ((Integer) attributeExpressionExecutors[7].execute(null));
                    if (maxInstance == -1) {
                        maxInstance = Integer.MAX_VALUE;
                    }
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the eighth argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[7].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Maximum events count" +
                        " must be a constant");
            }
        }

        parameterPosition = parameterWidth;
        numberOfNumerics = numberOfAttributes - numberOfNominals - 1;
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();
        streamingClassification = new StreamingClassification(maxInstance, batchSize,
                numberOfClasses, numberOfAttributes, numberOfNominals, nominalAttributesValues,
                parallelism, numberModelsBagging);

        new Thread(streamingClassification).start();

        List<Attribute> attributes = new ArrayList<Attribute>(numberOfAttributes);
        for (int i = 0; i < numberOfAttributes - 1; i++) {
            if (i < numberOfNumerics) {
                attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
            } else {
                attributes.add(new Attribute("att_" + i, Attribute.Type.STRING));
            }
        }
        attributes.add(new Attribute("prediction", Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor
            nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater
                                   complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - parameterPosition];
                    Object evt;

                    // Set cep event values
                    String classValue =
                            (String) attributeExpressionExecutors[attributeExpressionLength - 1].
                                    execute(complexEvent).toString();
                    //Set class value
                    if (classValue.equals("?")) {           //these data points for prediction
                        cepEvent[numberOfAttributes - 1] = -1;
                    } else { // These data points have class values, therefore these data use to train and test the model
                        if (classes.contains(classValue)) {
                            cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                        } else {
                            if (classes.size() < numberOfClasses) {
                                classes.add(classValue);
                                cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                            }
                        }
                    }
                    int nominalIndex = 0;

                    // Set other attributes
                    for (int i = 0; i < numberOfAttributes - 1; i++) {
                        evt = attributeExpressionExecutors[i + parameterPosition].execute(complexEvent);

                        if (i < numberOfNumerics) {         // set Numerical attributes
                            cepEvent[i] = (Double) evt;
                        } else {                            // Set nominal attributes
                            String v = (String) evt;
                            if (nominals.size() > nominalIndex) {
                                if (!nominals.get(nominalIndex).contains(evt)) {
                                    nominals.get(nominalIndex).add(v);
                                }
                            } else {
                                nominals.add(new ArrayList<String>());
                                nominals.get(nominalIndex).add(v);
                            }
                            cepEvent[i] = nominals.get(nominalIndex).indexOf(v);
                            nominalIndex++;
                        }
                    }
                    streamingClassification.addEvents(cepEvent);

                    Object[] outputData = null;
                    outputData = streamingClassification.getOutput();

                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {       // If output have values, then add those values to output stream
                        int index_predic = (int) outputData[outputData.length - 1];
                        outputData[outputData.length - 1] = classes.get(index_predic);
                        if (numberOfNominals != 0) {
                            for (int k = numberOfNumerics; k < numberOfAttributes - 1; k++) {
                                int nominal_index = (int) outputData[k];
                                outputData[k] = nominals.get(k - numberOfNumerics).
                                        get(nominal_index);
                            }
                        }
                        StreamEvent streamEvent1 = new StreamEvent(0, 0,outputData.length);
                        streamEvent1.setOutputData(outputData);
                        complexEventChunk.add(streamEvent1);
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }


                } else { // Timer events for poll output events from prediction queue
                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    scheduler.notifyAt(lastScheduledTimestamp);
                    Object[] outputData = null;
                    outputData = streamingClassification.getOutput();
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        int index_predic = (int) outputData[outputData.length - 1];
                        outputData[outputData.length - 1] = classes.get(index_predic);
                        if (numberOfNominals != 0) {
                            for (int k = numberOfNumerics; k < numberOfAttributes - 1; k++) {
                                int nominal_index = (int) outputData[k];
                                outputData[k] = nominals.get(k - numberOfNumerics).
                                        get(nominal_index);
                            }
                        }

                        StreamEvent streamEvent1 = new StreamEvent(0, 0,outputData.length);
                        streamEvent1.setOutputData(outputData);
                        complexEventChunk.add(streamEvent1);
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                }
            }
        }
        nextProcessor.process(complexEventChunk);
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{numberOfAttributes, numberOfClasses, numberOfNominals,
                nominalAttributesValues, maxInstance, batchSize, parallelism, numberModelsBagging,
                parameterPosition, numberOfNumerics, scheduler, lastScheduledTimestamp, TIMER_DURATION,
                streamingClassification, classes, nominals};
    }

    @Override
    public void restoreState(Object[] state) {
        numberOfAttributes = (int) state[0];
        numberOfClasses = (int) state[1];
        numberOfNominals = (int) state[2];
        nominalAttributesValues = (String) state[3];
        maxInstance = (int) state[4];
        batchSize = (int) state[5];
        parallelism = (int) state[6];
        numberModelsBagging = (int) state[7];
        parameterPosition = (int) state[8];
        numberOfNumerics = (int) state[9];
        scheduler = (Scheduler) state[10];
        lastScheduledTimestamp = (long) state[11];
        TIMER_DURATION = (long) state[12];
        streamingClassification = (StreamingClassification) state[13];
        classes = (List<String>) state[14];
        nominals = (ArrayList<ArrayList<String>>) state[15];
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp > 0) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() +
                    TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }
}
