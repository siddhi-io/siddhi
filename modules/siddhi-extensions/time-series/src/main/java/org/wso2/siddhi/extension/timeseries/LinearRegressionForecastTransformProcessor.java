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
@SiddhiExtension(namespace = "timeseries", function = "forecast")

public class LinearRegressionForecastTransformProcessor extends TransformProcessor
{

    static final Logger log = Logger.getLogger(LinearRegressionForecastTransformProcessor.class);

    private int paramCount = 0;         // Number of x variables +1
    private int calcInterval = 1;       // how frequently regression is calculated
    private int batchSize = 1000000000; //maximum number of events for a regression calculation
    private double xDash;               // input variable for forecasting
    private double ci = 0.95;           // confidence interval
    private final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2;
    private Map<Integer, String> paramPositions = new HashMap<Integer, String>();
    private RegressionCalculator regressionCalculator = null;

    public LinearRegressionForecastTransformProcessor() {
    }

    @Override
    protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {


        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("linregStream");
            this.outStreamDefinition.attribute("forecastY", Attribute.Type.DOUBLE);
        }

        calcInterval = ((IntConstant) parameters[0]).getValue();
        batchSize = ((IntConstant) parameters[1]).getValue();
        ci = ((DoubleConstant) parameters[2]).getValue();
        xDash = ((DoubleConstant) parameters[3]).getValue();

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
        log.debug("processEvent");
        Object[] outStreamData=null;
        Object[] regResults = regressionCalculator.linearRegressionCalculation(inEvent, paramPositions, paramCount, calcInterval, batchSize, ci);
        if(regResults!=null) {

            double forecastY = ((Number) regResults[1]).doubleValue()+ xDash * ((Number) regResults[2]).doubleValue();
            outStreamData = new Object[]{forecastY};
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
        if (objects.length > 0 && objects[0] instanceof Map) {  //WHAT IS THIS IF CONDITION FOR?
        }
    }

    @Override
    public void destroy() {
        regressionCalculator.close();
    }

}
