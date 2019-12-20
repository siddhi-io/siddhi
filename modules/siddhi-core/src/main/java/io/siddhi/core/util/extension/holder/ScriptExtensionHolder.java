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
import io.siddhi.core.function.Script;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Holder to store {@link Script} Extensions.
 */
public class ScriptExtensionHolder extends AbstractExtensionHolder {
    private static Class clazz = Script.class;

    protected ScriptExtensionHolder(SiddhiAppContext siddhiAppContext) {
        super(clazz, siddhiAppContext);
    }

    public static ScriptExtensionHolder getInstance(SiddhiAppContext siddhiAppContext) {
        ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap =
                siddhiAppContext.getSiddhiContext().getExtensionHolderMap();
        AbstractExtensionHolder abstractExtensionHolder = extensionHolderMap.get(clazz);
        if (abstractExtensionHolder == null) {
            abstractExtensionHolder = new ScriptExtensionHolder(siddhiAppContext);
            extensionHolderMap.putIfAbsent(clazz, abstractExtensionHolder);
        }
        return (ScriptExtensionHolder) extensionHolderMap.get(clazz);
    }
}
