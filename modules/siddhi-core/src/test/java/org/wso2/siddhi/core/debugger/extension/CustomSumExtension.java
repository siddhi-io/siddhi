package org.wso2.siddhi.core.debugger.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by bhagya on 7/21/16.
 */
public class CustomSumExtension extends FunctionExecutor {
    private Attribute.Type returnType;
    double total = 0;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            Attribute.Type attributeType = expressionExecutor.getReturnType();
            if (attributeType == Attribute.Type.DOUBLE) {
                returnType = attributeType;

            } else if ((attributeType == Attribute.Type.STRING) || (attributeType == Attribute.Type.BOOL)) {
                throw new ExecutionPlanCreationException("Plus cannot have parameters with types String or Bool");
            } else {
                returnType = Attribute.Type.LONG;
            }
        }
    }

    @Override
    protected Object execute(Object[] data) {
        for (Object aObj : data) {
            total += Double.parseDouble(String.valueOf(aObj));
        }

        return total;
    }

    @Override
    protected Object execute(Object data) {
        total = Double.parseDouble(String.valueOf(data));

        return total;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public Object[] currentState() {
        return new Object[]{total};
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
