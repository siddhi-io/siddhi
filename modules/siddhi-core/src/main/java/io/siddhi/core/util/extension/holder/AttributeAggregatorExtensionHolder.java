/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.util.extension.holder;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Holder to store {@link AttributeAggregatorExecutor} Extensions.
 */
public class AttributeAggregatorExtensionHolder extends AbstractExtensionHolder {
    private static Class clazz = AttributeAggregatorExecutor.class;

    private AttributeAggregatorExtensionHolder(SiddhiAppContext siddhiAppContext) {
        super(clazz, siddhiAppContext);
    }

    public static AttributeAggregatorExtensionHolder getInstance(SiddhiAppContext siddhiAppContext) {
        ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap = siddhiAppContext.getSiddhiContext
                ().getExtensionHolderMap();
        AbstractExtensionHolder abstractExtensionHolder = extensionHolderMap.get(clazz);
        if (abstractExtensionHolder != null && siddhiAppContext.getSiddhiContext().getSiddhiExtensions().size() !=
                abstractExtensionHolder.extensionMap.size()) {
            extensionHolderMap.remove(clazz);
            abstractExtensionHolder = new AttributeAggregatorExtensionHolder(siddhiAppContext);
        } else if (abstractExtensionHolder == null) {
            abstractExtensionHolder = new AttributeAggregatorExtensionHolder(siddhiAppContext);
        }
        extensionHolderMap.putIfAbsent(clazz, abstractExtensionHolder);
        return (AttributeAggregatorExtensionHolder) extensionHolderMap.get(clazz);
    }
}
