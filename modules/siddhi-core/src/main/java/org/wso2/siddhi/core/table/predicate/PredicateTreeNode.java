package org.wso2.siddhi.core.table.predicate;

import java.util.List;

public interface PredicateTreeNode {

    /**
     * Resulting predicate of the tree starting from this node
     * @return
     */
    public String buildPredicateString();

    /**
     * In-order traversed parameter list
     * @param parametersList
     */
    public void populateParameters(List parametersList);


    /**
     *
     * @param tokens
     */
    public void populateTokens(List tokens);

}
