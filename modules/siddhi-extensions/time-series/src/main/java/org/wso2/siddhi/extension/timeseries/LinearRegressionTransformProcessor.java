package org.wso2.siddhi.extension.timeseries;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.extension.timeseries.linreg.MultipleLinearRegressionCalculator;
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
@SiddhiExtension(namespace = "timeseries", function = "regress")

// TODO: explain function paramters

public class LinearRegressionTransformProcessor extends TransformProcessor
{

    static final Logger log = Logger.getLogger(LinearRegressionTransformProcessor.class);

    private int paramCount = 0;         // Number of x variables +1
    private int calcInterval = 1;       // The frequency of regression calculation
    private int batchSize = 1000000000; // Maximum # of events, used for regression calculation
    private double ci = 0.95;           // Confidence Interval
    private final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2;
    private Map<Integer, String> paramPositions = new HashMap<Integer, String>();   // Capture Input Parameters
    private RegressionCalculator regressionCalculator = null;

    public LinearRegressionTransformProcessor() {
    }

    @Override
    protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {

        if (log.isDebugEnabled()) {
            // TODO: print the stream values
            log.debug("Query Initialized");
        }
        // TODO: Try to get defaults
        calcInterval = ((IntConstant) parameters[0]).getValue();
        batchSize = ((IntConstant) parameters[1]).getValue();
        ci = ((DoubleConstant) parameters[2]).getValue();

        // TODO: exception handling: type for constants, attribute count for variables

        // processing siddhi query
        // TODO: start for loop from 3rd parameter
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
            // TODO: send constants to regression calculators here itself as parameters
            regressionCalculator = new MultipleLinearRegressionCalculator();
        } else {
            regressionCalculator = new SimpleLinearRegressionCalculator();
        }

        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("linregStream");
            this.outStreamDefinition.attribute("stdError", Attribute.Type.DOUBLE);

            // Creating the outstream attributes based on the number of input parameters
            String betaVal;
            for (int itr = 0; itr < paramCount ; itr++) {
                betaVal = "beta" + itr;
                this.outStreamDefinition.attribute(betaVal, Attribute.Type.DOUBLE);

            }
            // TODO: add the input variables to the outstream, by iterating input stream definition
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (log.isDebugEnabled()) {
            log.debug("processEvent");
        }
        return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),  regressionCalculator.calculateLinearRegression(inEvent, paramPositions, paramCount, calcInterval, batchSize, ci));
    }
    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        // TODO: Use processEvent for all events, get results and put in to list event and send it out
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
