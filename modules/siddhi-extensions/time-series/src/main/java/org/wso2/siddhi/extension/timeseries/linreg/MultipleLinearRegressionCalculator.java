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
    private int perfCounter =0;

    public MultipleLinearRegressionCalculator() {
//        init();
    }

    public void init() {
    }

    public void close() {
    }

    public Object[] linearRegressionCalculation ( InEvent inEvent, Map<Integer, String> paramPositions, int paramCount, int limit, double ci) {

        confidenceInterval = ci;
        batchSize = limit;

        addEvent(inEvent, paramPositions, paramCount);

        if(eventCount > batchSize){
            eventCount--;
            removeEvent();
        }

        //FOR PERFORMANCE TEST PURPOSES
        if(perfCounter%1000 != 0){
            return null;
        }
        return  processData();
    }

    public void addEvent (InEvent inEvent, Map<Integer, String> paramPositions, int paramCount) {

        perfCounter++;
        eventCount++;
        double[] dataX = new double[paramCount];
        double[] dataY = new double[1];
        int itr = 0;

        dataX[0] = 1.0;

        for (Map.Entry<Integer, String> entry : paramPositions.entrySet()) {

           if (itr == 0) {
                dataY[0] =  Double.parseDouble(inEvent.getData(entry.getKey()).toString());

            }
            else {
                dataX[itr] = Double.parseDouble(inEvent.getData(entry.getKey()).toString());
           }
            itr++;
        }

        xValueList.add(dataX);
        yValueList.add(dataY);
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
        int outputDataCount = 1 + (xParameterCount + 1) * 2;
        Object[] dataObjArray = new Object[outputDataCount];

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
            double stdErr = Math.sqrt(mse);
            dataObjArray[0] = stdErr;
            TDistribution t = new TDistribution(df);

            //Calculating beta errors and tstats
            for(int j=0; j <= xParameterCount; j++) {
                betaErrors[j] = Math.sqrt(matXTXInverse.get(j,j) * mse);
                tStats[j] = matBetas.get(j,0)/betaErrors[j];

                pValue = 1-(t.cumulativeProbability(Math.abs(tStats[j])) - 1 + t.cumulativeProbability(Math.abs(tStats[j])));
                if ( pValue > p) {
                    matBetas.set(j,0,0);
                }

                dataObjArray[j+1] = matBetas.get(j,0);
                dataObjArray[j+2+ xParameterCount] = tStats[j];
            }
        }
        catch(RuntimeException e){
            dataObjArray[0]="Insufficient Data";
        }

        return dataObjArray;
    }
}