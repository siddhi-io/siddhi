package org.wso2.siddhi.extension.timeseries.linreg;

import Jama.Matrix;
import org.apache.commons.math3.distribution.TDistribution;
import org.wso2.siddhi.core.event.in.InEvent;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by seshika on 4/9/14.
 */
public class MultipleLinearRegressionCalculator extends RegressionCalculator{

    private List<double[]> yValueList = new LinkedList<double[]>();
    private List<double[]> xValueList = new LinkedList<double[]>();
    private double confidenceInterval = 0.0;
    private int eventCount = 0;
    private int xParameterCount = 0;
    private int batchSize = 1000000000;
    private int incCounter =0;

    public MultipleLinearRegressionCalculator() {
    }

    public void init() {
    }

    public void close() {
    }

    public Object[] calculateLinearRegression(InEvent inEvent, Map<Integer, String> paramPositions, int paramCount, int calcInterval, int limit, double ci) {

        confidenceInterval = ci;
        batchSize = limit;

        // TODO: implement this at the super class

        addEvent(inEvent, paramPositions, paramCount);

        // removing oldest events in order to maintain batchsize
        if(eventCount > batchSize){
            eventCount--;
            removeEvent();
        }

        // processing at a user specified calculation interval
        if(incCounter % calcInterval != 0){
            return null;
        }
        return  processData();
    }

    public void addEvent (InEvent inEvent, Map<Integer, String> paramPositions, int paramCount) {

        incCounter++;
        eventCount++;
        double[] dataX = new double[paramCount];
        double[] dataY = new double[1]; // TODO: check whether double can be used instead of array
        int itr = 0;

        dataX[0] = 1.0;

        // TODO: iterate from 1st position, take the Y out of the loop
        for (Map.Entry<Integer, String> entry : paramPositions.entrySet()) {

           if (itr == 0) {
                dataY[0] =  ((Number) inEvent.getData(entry.getKey())).doubleValue();
           } else {
                dataX[itr] = ((Number) inEvent.getData(entry.getKey())).doubleValue();
           }
           itr++;
        }

        xValueList.add(dataX);
        yValueList.add(dataY);
        // TODO: move this to the constructor
        xParameterCount = paramCount - 1;
    }

    public void removeEvent(){

        xValueList.remove(0);
        yValueList.remove(0);
    }

    public Object[] processData() {

        double[][] xArray = xValueList.toArray(new double[eventCount][xParameterCount +1]);
        double[][] yArray = yValueList.toArray(new double[eventCount][1]);
        double [] betaErrors = new double[xParameterCount +1];
        double [] tStats = new double[xParameterCount +1];
        double sse = 0.0;                               // sum of square error
        double df = eventCount - xParameterCount - 1;   // Degrees of Freedom for Confidence Interval
        double p = 1- confidenceInterval;               // P value of specified confidence interval
        double pValue;
        Object[] regResults = new Object[xParameterCount + 2];

        // Calculate Betas
        try{

            Matrix matY = new Matrix(yArray);
            Matrix matX = new Matrix(xArray);
            Matrix matXTranspose = matX.transpose();
            Matrix matXTXInverse = matXTranspose.times(matX).inverse();
            Matrix matBetas = matXTXInverse.times(matXTranspose).times(matY);

            Matrix yHat = matX.times(matBetas);

            // Calculate Sum of Squares
            for (int i = 0; i < eventCount; i++) {
                sse += Math.pow((yHat.get(i,0) - yArray[i][0]),2);
            }

            // Calculating Errors
            double mse = sse/df;
            regResults[0] = Math.sqrt(mse);      // Standard Error of Regression
            TDistribution t = new TDistribution(df);

            // Calculating beta errors and tstats
            for(int j=0; j <= xParameterCount; j++) {
                betaErrors[j] = Math.sqrt(matXTXInverse.get(j,j) * mse);
                tStats[j] = matBetas.get(j,0)/betaErrors[j];

                // Eliminating statistically weak coefficients
                pValue = 2 * (1 - t.cumulativeProbability(Math.abs(tStats[j])));
                if ( pValue > p) {
                    regResults[j+1] = 0;
                }else {
                    regResults[j+1] = matBetas.get(j,0);
                }
            }
        }
        catch(RuntimeException e){
            return null;
            // TODO: create dummy obj array and send
        }

        return regResults;
    }
}