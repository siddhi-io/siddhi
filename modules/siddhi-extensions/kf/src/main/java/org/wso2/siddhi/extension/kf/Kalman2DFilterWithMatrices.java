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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.kf;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kalman2DFilter(measuredXValue, measuredYValue, timestamp, objectId, level)
 * These methods attempts to find the next sub-sequence of the 'inputSequence' that matches the 'regex' pattern.
 * kalman2DFilter - estimate values for noisy data.
 *   measuredXValue - measured value X eg:40.695881
 *   measuredYValue - measured value Y eg:-74.178444
 *   measuredChangingRate - Changing rate. eg: Velocity of the point which describes from measured value X and Y - 0.003d meters per second
 *   measurementNoiseSD - standard deviation of the noise. eg: 0.01
 *   timestamp - the timestamp at the measured time eg: 1445234861l
 *   objectId - object id of the measured object eg: id1
 *   level - if the object is in a building with floors, the floor number will be the level eg: 1, 2, etc.
 * Accept Type(s) for kalman2DFilter(measuredXValue, measuredYValue, measuredChangingRate, timestamp, objectId, level);
 *   measuredXValue : DOUBLE
 *   measuredYValue : DOUBLE
 *   measuredChangingRate : DOUBLE
 *   measurementNoiseSD : DOUBLE
 *   timestamp : LONG
 *   objectId : STRING
 *   level : INT
 * Return Type(s): DOUBLE, DOUBLE
 */
public class Kalman2DFilterWithMatrices extends StreamFunctionProcessor {

    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, KalmonFilter>> kalmonFilterHashMap;

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }

    @Override
    protected Object[] process(Object[] data) {
        if (data[0] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. First argument cannot be null");
        }
        if (data[1] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Second argument cannot be null");
        }
        if (data[2] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Third argument cannot be null");
        }
        if (data[3] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Fourth argument cannot be null");
        }
        if (data[4] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Fifth argument cannot be null");
        }
        if (data[5] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Sixth argument cannot be null");
        }
        if (data[6] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to kf:kalman2DFilter() " +
                                                    "function. Seventh argument cannot be null");
        }

        double measuredXValue = (Double) data[0];
        double measuredYValue = (Double) data[1];
        double measuredChangingRate = (Double) data[2];
        double measurementNoiseSD = (Double) data[3];
        long timestamp = (Long) data[4];
        String objectId = (String) data[5];
        int level = (Integer) data[6]; //level should be the same if the all the objects are on the same plane
        ConcurrentHashMap<Integer, KalmonFilter> levelSpecificKalmonFilterMap = kalmonFilterHashMap.get(objectId);

        if (levelSpecificKalmonFilterMap == null) {
            kalmonFilterHashMap.put(objectId, new ConcurrentHashMap<Integer, KalmonFilter>());
        }

        KalmonFilter kalmonFilter = kalmonFilterHashMap.get(objectId).get(level);

        long timestampDiff;
        RealMatrix transitionMatrixA;
        RealMatrix measurementMatrixH;
        RealMatrix varianceMatrixP;
        double[][] measuredValues = {{measuredXValue}, {measuredChangingRate}};
        double[][] measuredValuesY = {{measuredYValue}, {measuredChangingRate}};
        RealMatrix prevMeasuredMatrixX;
        RealMatrix prevMeasuredMatrixY;
        double[][] Rvalues = {{measurementNoiseSD, 0}, {0, measurementNoiseSD}};
        RealMatrix Rmatrix = MatrixUtils.createRealMatrix(Rvalues);
        long prevTimestamp;

        if (kalmonFilter == null) {
            timestampDiff = 1;
            double[][] varianceValues = {{1000, 0}, {0, 1000}};
            double[][] measurementValues = {{1, 0}, {0, 1}};
            measurementMatrixH = MatrixUtils.createRealMatrix(measurementValues);
            kalmonFilter = new KalmonFilter();
            varianceMatrixP = MatrixUtils.createRealMatrix(varianceValues);
            prevMeasuredMatrixX = MatrixUtils.createRealMatrix(measuredValues);
            prevMeasuredMatrixY = MatrixUtils.createRealMatrix(measuredValuesY);
        } else {
            prevTimestamp = kalmonFilter.getPrevTimestamp();
            timestampDiff = (timestamp - prevTimestamp);
            prevMeasuredMatrixX = kalmonFilter.getPrevMeasuredMatrixX();
            prevMeasuredMatrixY = kalmonFilter.getPrevMeasuredMatrixY();
            measurementMatrixH = kalmonFilter.getMeasurementMatrixH();
            varianceMatrixP = kalmonFilter.getVarianceMatrixP();
        }

        double[][] transitionValues = {{1d, timestampDiff}, {0d, 1d}};
        transitionMatrixA = MatrixUtils.createRealMatrix(transitionValues);

        RealMatrix measuredMatrixX = MatrixUtils.createRealMatrix(measuredValues);
        RealMatrix measuredMatrixY = MatrixUtils.createRealMatrix(measuredValuesY);

        //Xk = (A * Xk-1)
        prevMeasuredMatrixX = transitionMatrixA.multiply(prevMeasuredMatrixX);
        prevMeasuredMatrixY = transitionMatrixA.multiply(prevMeasuredMatrixY);

        //Pk = (A * P * AT) + Q
        varianceMatrixP = (transitionMatrixA.multiply(varianceMatrixP)).multiply(transitionMatrixA.transpose());

        //S = (H * P * HT) + R
        RealMatrix S = ((measurementMatrixH.multiply(varianceMatrixP)).multiply(measurementMatrixH.transpose())).add(Rmatrix);
        RealMatrix S_1 = new LUDecomposition(S).getSolver().getInverse();

        //P * HT * S-1
        RealMatrix kalmanGainMatrix = (varianceMatrixP.multiply(measurementMatrixH.transpose())).multiply(S_1);

        //Xk = Xk + kalmanGainMatrix (Zk - HkXk )
        prevMeasuredMatrixX = prevMeasuredMatrixX.add(kalmanGainMatrix.multiply(
                (measuredMatrixX.subtract(measurementMatrixH.multiply(prevMeasuredMatrixX)))));

        prevMeasuredMatrixY = prevMeasuredMatrixY.add(kalmanGainMatrix.multiply(
                (measuredMatrixY.subtract(measurementMatrixH.multiply(prevMeasuredMatrixY)))));

        //Pk = Pk - K.Hk.Pk
        varianceMatrixP = varianceMatrixP.subtract(
                (kalmanGainMatrix.multiply(measurementMatrixH)).multiply(varianceMatrixP));

        prevTimestamp = timestamp;
        kalmonFilter.setPrevMeasuredMatrixX(prevMeasuredMatrixX);
        kalmonFilter.setPrevMeasuredMatrixY(prevMeasuredMatrixY);
        kalmonFilter.setVarianceMatrixP(varianceMatrixP);
        kalmonFilter.setPrevTimestamp(prevTimestamp);
        kalmonFilter.setMeasurementMatrixH(measurementMatrixH);
        kalmonFilterHashMap.get(objectId).put(level, kalmonFilter);
        return new Object[]{prevMeasuredMatrixX.getRow(0)[0], prevMeasuredMatrixY.getRow(0)[0]};
    }

    @Override
    protected Object[] process(Object data) {
        return new Object[0];
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        kalmonFilterHashMap = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, KalmonFilter>>();
        if (attributeExpressionExecutors.length != 7) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to " +
                                                       "kf:kalman2DFilter() function, " +
                                                       "requires 7, but found " + attributeExpressionExecutors.length);
        }
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(2);
        attributes.add(new Attribute("kalmonLatitude", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("kalmonLongitude", Attribute.Type.DOUBLE));
        return attributes;
    }

    private class KalmonFilter {
        private double[][] measurementValues = {{1, 0}, {0, 1}};
        private RealMatrix measurementMatrixH = MatrixUtils.createRealMatrix(measurementValues);
        private RealMatrix varianceMatrixP;
        private RealMatrix prevMeasuredMatrixX;
        private RealMatrix prevMeasuredMatrixY;
        private long prevTimestamp;

        public RealMatrix getVarianceMatrixP() {
            return varianceMatrixP;
        }

        public void setVarianceMatrixP(RealMatrix varianceMatrixP) {
            this.varianceMatrixP = varianceMatrixP;
        }

        public RealMatrix getPrevMeasuredMatrixX() {
            return prevMeasuredMatrixX;
        }

        public void setPrevMeasuredMatrixX(RealMatrix prevMeasuredMatrixX) {
            this.prevMeasuredMatrixX = prevMeasuredMatrixX;
        }

        public RealMatrix getPrevMeasuredMatrixY() {
            return prevMeasuredMatrixY;
        }

        public void setPrevMeasuredMatrixY(RealMatrix prevMeasuredMatrixY) {
            this.prevMeasuredMatrixY = prevMeasuredMatrixY;
        }

        public long getPrevTimestamp() {
            return prevTimestamp;
        }

        public void setPrevTimestamp(long prevTimestamp) {
            this.prevTimestamp = prevTimestamp;
        }

        public RealMatrix getMeasurementMatrixH() {
            return measurementMatrixH;
        }

        public void setMeasurementMatrixH(RealMatrix measurementMatrixH) {
            this.measurementMatrixH = measurementMatrixH;
        }
    }
}
