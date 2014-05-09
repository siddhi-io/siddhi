package org.wso2.siddhi.extension.timeseries.linreg;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.in.InEvent;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by waruna on 4/9/14.
 */
public class SimpleLinearRegressionCalculator extends RegressionCalculator{

    static final Logger log = Logger.getLogger(SimpleLinearRegressionCalculator.class);
    private List<Double> xValueList = new LinkedList<Double>();
    private List<Double> yValueList = new LinkedList<Double>();
    private int eventCount = 0;
    private double sumX = 0.0, sumY = 0.0, sumxsquared = 0.0;
    private double confidenceInterval = 0.0;
    private int batchSize = 1000000000;
    private int perfCounter =0;

    public SimpleLinearRegressionCalculator () {
    }

    public void init () {
    }

    public void close () {
    }

    public Object[] linearRegressionCalculation ( InEvent inEvent, Map<Integer, String> paramPositions, int paramCount, int limit, double ci ) {


        confidenceInterval = ci;
        batchSize = limit;

        addEvent(inEvent, paramPositions, paramCount);

        // removing oldest events in order to maintain batchsize
        if( eventCount > batchSize){
            eventCount--;
            removeEvent();
        }

        //FOR PERFORMANCE TEST PURPOSES
        if(perfCounter%10000000 != 0){
            return null;
        }
        return  processData();
    }

    public void addEvent (InEvent inEvent, Map<Integer, String> paramPositions, int paramCount) {

        eventCount++;
        perfCounter++;

        int itr = 0;
        double xValue = 0.0 , yValue = 0.0;

        for (Map.Entry<Integer, String> entry : paramPositions.entrySet()) {

            if (itr == 0) {
                yValue = Double.parseDouble(inEvent.getData(entry.getKey()).toString());
                yValueList.add(yValue);
            }
            else {
                xValue = Double.parseDouble(inEvent.getData(entry.getKey()).toString());
                xValueList.add(xValue);
            }
            itr++;
        }

        sumX += xValue;
        sumxsquared += Math.pow(xValue, 2);
        sumY  += yValue;
    }

    public void removeEvent(){

        sumX -= xValueList.get(0);
        sumxsquared -= Math.pow(xValueList.get(0), 2);
        sumY  -= yValueList.get(0);

        xValueList.remove(0);
        yValueList.remove(0);
    }

    public Object[] processData ()
    {


        Object[] dataObj = new Object[5];
        try
        {
            Double[] xArray = xValueList.toArray(new Double[eventCount]);
            Double[] yArray = yValueList.toArray(new Double[eventCount]);

            double xbar, ybar, xxbar = 0.0, yybar = 0.0, xybar = 0.0, beta1, beta0, R2, stderr, beta1err, beta0err, t_beta0, t_beta1, fit;

            double resss = 0.0;      // residual sum of squares
            double regss = 0.0;      // regression sum of squares
            int df = eventCount - 2;      // degrees of freedom (n-k-1)
            TDistribution t = new TDistribution(df);

            //  compute xbar and ybar
            xbar = sumX / eventCount;
            ybar = sumY / eventCount;

            // compute summary statistics
            for (int i = 0; i < eventCount; i++)
            {
                xxbar += (xArray[i] - xbar) * (xArray[i] - xbar);
                yybar += (yArray[i] - ybar) * (yArray[i] - ybar);
                xybar += (xArray[i] - xbar) * (yArray[i] - ybar);
            }

            //compute coefficients
            beta1 = xybar / xxbar; // output param 1
            beta0 = ybar - beta1 * xbar; // output param 2

            // analyze results
            for (int i = 0; i < eventCount; i++)
            {
                fit = beta1 * xArray[i] + beta0;
                resss += (fit - yArray[i]) * (fit - yArray[i]);
                regss += (fit - ybar) * (fit - ybar);
            }

            //calculationg standard errors
            R2 = regss / yybar;
            stderr = Math.sqrt(resss / df); // output param 3
            beta1err = stderr / Math.sqrt(xxbar);
            beta0err = stderr * Math.sqrt(sumxsquared / (eventCount * xxbar));

            //calculating tstats
            t_beta0 = beta0 / beta0err;
            t_beta1 = beta1 / beta1err;

            // Eliminating weak coefficiants
            double pHighValue, pLowValue, pValue;

            pHighValue = t.cumulativeProbability(Math.abs(t_beta0));
            pLowValue = 1 - t.cumulativeProbability(Math.abs(t_beta0));
            pValue = 1 - (pHighValue - pLowValue);

            if (pValue > (1 - confidenceInterval))
            {
                beta0 = 0;
            }

            pHighValue = t.cumulativeProbability(Math.abs(t_beta1));
            pLowValue = 1 - t.cumulativeProbability(Math.abs(t_beta1));
            pValue = 1 - (pHighValue - pLowValue);

            if (pValue > (1 - confidenceInterval))
            {
                beta1 = 0;
            }

            dataObj = new Object[]{stderr, beta0, beta1, t_beta0, t_beta1};

            log.debug(dataObj);

        } catch (Exception e)
        {
            return null;
        }
        return dataObj;
    }
}
