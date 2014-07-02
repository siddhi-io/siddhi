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

/**
 * The methods supported by this function are
 * timeseries:regress(int/long/float/double y, int/long/float/double x1, int/long/float/double x2 ...)
 * and
 * timeseries:regress(int calcInterval, int batchSize, double confidenceInterval, int/long/float/double y, int/long/float/double x1, int/long/float/double x2 ...)
 *
 * where
 * @param calcInterval      Frequency of regression calculation
 * @param batchSize         Maximum number of events, used for regression calculation
 * @param confidenceInterval Confidence interval to be used for regression calculation
 * @param y                 Dependant variable
 * @param x1~xn             Independant variables
 */

public class LinearRegressionTransformProcessor extends TransformProcessor
{
    static final Logger log = Logger.getLogger(LinearRegressionTransformProcessor.class);

    private int paramCount = 0;                                         // Number of x variables +1
    private int calcInterval = 1;                                       // The frequency of regression calculation
    private int batchSize = 1000000000;                                 // Maximum # of events, used for regression calculation
    private double ci = 0.95;                                           // Confidence Interval
    private final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2;              // Number of Input parameters in a simple linear regression
    private Object [] regResult;                                        // Calculated Regression Coefficients
    private Map<Integer, String> paramPositions = new HashMap<Integer, String>();   // Input Parameters
    private RegressionCalculator regressionCalculator = null;

    public LinearRegressionTransformProcessor() {
    }

    @Override
    protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {

        if (log.isDebugEnabled()) {
            log.debug("Query Initialized. Stream Parameters: " + inStreamDefinition.toString());
        }

        // Capture constant inputs
        if(parameters[0] instanceof IntConstant) {
            try {
                calcInterval = ((IntConstant) parameters[0]).getValue();
                batchSize = ((IntConstant) parameters[1]).getValue();
            } catch(ClassCastException c) {
                throw new QueryCreationException("Calculation interval, batch size and range should be of type int");
            }
            try {
                ci = ((DoubleConstant) parameters[2]).getValue();
            } catch(ClassCastException c) {
                throw new QueryCreationException("Confidence interval should be of type double and a value between 0 and 1");
            }

            // Capture variable inputs
            for (int i=3; i<parameters.length; i++) {
                if (parameters[i] instanceof Variable) {
                    Variable var = (Variable) parameters[i];
                    String attributeName = var.getAttributeName();
                    paramPositions.put(inStreamDefinition.getAttributePosition(attributeName), attributeName );
                    paramCount++;
                }
            }
        } else {
            // Capture variable inputs
            for (int i=0; i<parameters.length; i++) {
                if (parameters[i] instanceof Variable) {
                    Variable var = (Variable) parameters[i];
                    String attributeName = var.getAttributeName();
                    paramPositions.put(inStreamDefinition.getAttributePosition(attributeName), attributeName );
                    paramCount++;
                }
            }
        }

        // Pick the appropriate regression calculator
        if(paramCount > SIMPLE_LINREG_INPUT_PARAM_COUNT) {
            regressionCalculator = new MultipleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        } else {
            regressionCalculator = new SimpleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        }

        // Create outstream
        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("linregStream");

            // Create outstream attribute to send standard error of regression
            this.outStreamDefinition.attribute("stdError", Attribute.Type.DOUBLE);

            // Create outstream attributes based on the number of regression coefficients
            String betaVal;
            for (int itr = 0; itr < paramCount ; itr++) {
                betaVal = "beta" + itr;
                this.outStreamDefinition.attribute(betaVal, Attribute.Type.DOUBLE);
            }

            // Create outstream attributes for all the attributes in the input stream
            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }


    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (log.isDebugEnabled()) {
            log.debug("processEvent");
        }

        // Capture all data elements in the input stream
        Object [] inStreamData = inEvent.getData();
        Object [] outStreamData;

        // Perform Regression and get regression results
        Object [] temp = regressionCalculator.calculateLinearRegression(inEvent, paramPositions);

        // Send null until the first regression calculation
        if(regResult==null && temp==null){
            outStreamData = null;
        } else {
            // For each calculation, get new regression coefficients, otherwise send previous coefficients
            if(temp!=null) {
                regResult = temp;
            }

            // Combine Regression Results and Input Stream Data and send to OutputStream
            outStreamData = new Object[regResult.length + inStreamData.length];
            System.arraycopy(regResult, 0, outStreamData, 0, regResult.length);
            System.arraycopy(inStreamData, 0, outStreamData, regResult.length, inStreamData.length);
        }

        return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),  outStreamData);
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InStream inStream = null;

        // Perform processEvent() for each event in the list
        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                inStream = processEvent((InEvent) event);
            }
        }
        return inStream;
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
    }

}
