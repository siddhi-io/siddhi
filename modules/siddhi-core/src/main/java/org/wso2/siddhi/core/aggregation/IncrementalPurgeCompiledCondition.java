package org.wso2.siddhi.core.aggregation;

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.SortedMap;

public class IncrementalPurgeCompiledCondition implements CompiledCondition {
    private String compiledQuery;
    private SortedMap<Integer, Object> parameters;

    public IncrementalPurgeCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        return new IncrementalPurgeCompiledCondition(this.compiledQuery,this.parameters);
    }
    public String getCompiledQuery() {
        return compiledQuery;
    }

    public String toString() {
        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }
}
