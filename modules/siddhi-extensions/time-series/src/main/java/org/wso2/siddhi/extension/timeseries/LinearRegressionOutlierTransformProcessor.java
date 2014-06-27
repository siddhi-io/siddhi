package org.wso2.siddhi.extension.timeseries;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.extension.timeseries.linreg.RegressionCalculator;
import org.wso2.siddhi.extension.timeseries.linreg.SimpleLinearRegressionCalculator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.DoubleConstant;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by seshika on 4/9/14.
 */
@SiddhiExtension(namespace = "timeseries", function = "outlier")

public class LinearRegressionOutlierTransformProcessor extends TransformProcessor
{

    static final Logger log = Logger.getLogger(LinearRegressionOutlierTransformProcessor.class);

    private int eventCount = 0;         // Number of events added
    private int paramCount = 0;         // Number of x variables +1
    private int calcInterval = 1;       // The frequency of regression calculation
    private int batchSize = 1000000000; // Maximum # of events, used for regression calculation
    private double ci = 0.95;           // Confidence Interval
    private int range = 1;              // Number of standard deviations for outlier calc
    private Object[] regResults;
    private final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2;
    private Map<Integer, String> paramPositions = new HashMap<Integer, String>();
    private RegressionCalculator regressionCalculator = null;

    public LinearRegressionOutlierTransformProcessor() {
    }

    @Override
    protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {

        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("linregStream");
            this.outStreamDefinition.attribute("outlier", Attribute.Type.BOOL);
            this.outStreamDefinition.attribute("stdError", Attribute.Type.DOUBLE);
            this.outStreamDefinition.attribute("beta0", Attribute.Type.DOUBLE);
            this.outStreamDefinition.attribute("beta1", Attribute.Type.DOUBLE);
        }

        calcInterval = ((IntConstant) parameters[0]).getValue();
        batchSize = ((IntConstant) parameters[1]).getValue();
        ci = ((DoubleConstant) parameters[2]).getValue();
        range = ((IntConstant) parameters[3]).getValue();

        // processing siddhi query
        for (Expression parameter : parameters) {
            if (parameter instanceof Variable) {
                Variable var = (Variable) parameter;
                String attributeName = var.getAttributeName();
                paramPositions.put(inStreamDefinition.getAttributePosition(attributeName), attributeName );
                paramCount++;
            }
        }

        // pick the appropriate regression calculator
        if(paramCount > SIMPLE_LINREG_INPUT_PARAM_COUNT) {
            throw new QueryCreationException("Outlier Function is available only for simple linear regression");
        }
        else {
            regressionCalculator = new SimpleLinearRegressionCalculator();
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (log.isDebugEnabled()) {
            log.debug("processEvent");
        }

        Object[] outStreamData = new Object[paramCount+2];
        String result = "normal"; // TODO: convert in to BOOL
        eventCount++;

        if(eventCount < 4) { // TODO: explain 4
            regResults = regressionCalculator.calculateLinearRegression(inEvent, paramPositions, paramCount, calcInterval, batchSize, ci);
            outStreamData[0] = result;
        }
        else {
            // Calculate the upper limit and the lower limit based on standard error and regression equation
            double forecastY = (Double) regResults[1] + ((Number) inEvent.getData1()).doubleValue() * (Double) regResults[2];
            double upLimit = (Double) regResults[0] * range + forecastY;
            double downLimit = - (Double) regResults[0] * range + forecastY;

            // Check whether next Y value is an outlier based on the next X value and the current regression equation
            double nextY = ((Number)inEvent.getData0()).doubleValue();
            if(nextY < downLimit || nextY > upLimit) {
                result = "outlier";
            }
            regResults = regressionCalculator.calculateLinearRegression(inEvent, paramPositions, paramCount, calcInterval, batchSize, ci);
            outStreamData[0] = result;
            System.arraycopy(regResults,0,outStreamData, 1, regResults.length);
        }

        return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), outStreamData);
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InEvent lastEvent = null;

        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                regressionCalculator.addEvent((InEvent) event, paramPositions, paramCount);
                lastEvent = (InEvent) event;
            }
        }

        return new InEvent(lastEvent.getStreamId(), System.currentTimeMillis(), regressionCalculator.processData());

    }
    @Override
    protected Object[] currentState() {
        return null;
    }
    @Override
    protected void restoreState(Object[] objects) {
    }

    @Override
    public void destroy() {
        regressionCalculator.close();
    }

}
