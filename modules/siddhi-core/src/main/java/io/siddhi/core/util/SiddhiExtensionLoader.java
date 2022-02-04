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

package io.siddhi.core.util;

import io.siddhi.annotation.Extension;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.executor.incremental.IncrementalAggregateBaseTimeFunctionExecutor;
import io.siddhi.core.executor.incremental.IncrementalShouldUpdateFunctionExecutor;
import io.siddhi.core.executor.incremental.IncrementalStartTimeEndTimeFunctionExecutor;
import io.siddhi.core.executor.incremental.IncrementalTimeGetTimeZone;
import io.siddhi.core.executor.incremental.IncrementalUnixTimeFunctionExecutor;
import io.siddhi.core.function.Script;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;
import io.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.stream.output.sink.distributed.DistributionStrategy;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.extension.holder.AbstractExtensionHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.atteo.classindex.ClassIndex;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.BundleListener;
import org.osgi.framework.wiring.BundleWiring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class used to load Siddhi extensions.
 */
public class SiddhiExtensionLoader {

    private static final Logger log = LogManager.getLogger(SiddhiExtensionLoader.class);
    private static final Class ATTRIBUTE_AGGREGATOR_EXECUTOR_CLASS = AttributeAggregatorExecutor.class;
    private static final Class DISTRIBUTION_STRATEGY_CLASS = DistributionStrategy.class;
    private static final Class FUNCTION_EXECUTOR_CLASS = FunctionExecutor.class;
    private static final Class INCREMENTAL_ATTRIBUTE_AGGREGATOR_CLASS = IncrementalAttributeAggregator.class;
    private static final Class SCRIPT_CLASS = Script.class;
    private static final Class SINK_CLASS = Sink.class;
    private static final Class SINK_MAPPER_CLASS = SinkMapper.class;
    private static final Class SOURCE_CLASS = Source.class;
    private static final Class SOURCE_MAPPER_CLASS = SourceMapper.class;
    private static final Class STREAM_FUNCTION_PROCESSOR_CLASS = StreamFunctionProcessor.class;
    private static final Class STREAM_PROCESSOR_CLASS = StreamProcessor.class;
    private static final Class TABLE_CLASS = Table.class;
    private static final Class WINDOW_PROCESSOR_CLASS = WindowProcessor.class;
    private static List<Class> extensionNameSpaceList = new ArrayList<>();

    static {
        extensionNameSpaceList.add(DISTRIBUTION_STRATEGY_CLASS);
        extensionNameSpaceList.add(SCRIPT_CLASS);
        extensionNameSpaceList.add(SINK_CLASS);
        extensionNameSpaceList.add(SINK_MAPPER_CLASS);
        extensionNameSpaceList.add(SOURCE_CLASS);
        extensionNameSpaceList.add(SOURCE_MAPPER_CLASS);
        extensionNameSpaceList.add(TABLE_CLASS);
        extensionNameSpaceList.add(ATTRIBUTE_AGGREGATOR_EXECUTOR_CLASS);
        extensionNameSpaceList.add(FUNCTION_EXECUTOR_CLASS);
        extensionNameSpaceList.add(INCREMENTAL_ATTRIBUTE_AGGREGATOR_CLASS);
        extensionNameSpaceList.add(STREAM_FUNCTION_PROCESSOR_CLASS);
        extensionNameSpaceList.add(STREAM_PROCESSOR_CLASS);
        extensionNameSpaceList.add(WINDOW_PROCESSOR_CLASS);
    }

    /**
     * Helper method to load the Siddhi extensions.
     *
     * @param siddhiExtensionsMap           reference map for the Siddhi extension
     * @param extensionHolderMap            reference map for the Siddhi extension holder
     * @param deprecatedSiddhiExtensionsMap reference map for the deprecated Siddhi extensions
     */
    public static void loadSiddhiExtensions(Map<String, Class> siddhiExtensionsMap,
                                            ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap,
                                            Map<String, Class> deprecatedSiddhiExtensionsMap) {
        loadLocalExtensions(siddhiExtensionsMap, extensionHolderMap, deprecatedSiddhiExtensionsMap);
        BundleContext bundleContext = ReferenceHolder.getInstance().getBundleContext();
        if (bundleContext != null) {
            loadExtensionOSGI(bundleContext, siddhiExtensionsMap, extensionHolderMap, deprecatedSiddhiExtensionsMap);
        }
    }

    /**
     * Load Extensions in OSGi environment.
     *
     * @param bundleContext                 OSGi bundleContext
     * @param siddhiExtensionsMap           reference map for the Siddhi extension
     * @param extensionHolderMap            reference map for the Siddhi extension holder
     * @param deprecatedSiddhiExtensionsMap reference map for the deprecated Siddhi extensions
     */
    private static void loadExtensionOSGI(BundleContext bundleContext,
                                          Map<String, Class> siddhiExtensionsMap,
                                          ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap,
                                          Map<String, Class> deprecatedSiddhiExtensionsMap) {
        ExtensionBundleListener extensionBundleListener
                = new ExtensionBundleListener(siddhiExtensionsMap, extensionHolderMap, deprecatedSiddhiExtensionsMap);
        bundleContext.addBundleListener(extensionBundleListener);
        extensionBundleListener.loadAllExtensions(bundleContext);
    }

    /**
     * Load Siddhi extensions in java non OSGi environment.
     *
     * @param siddhiExtensionsMap           reference map for the Siddhi extension
     * @param extensionHolderMap            reference map for the Siddhi extension holder
     * @param deprecatedSiddhiExtensionsMap reference map for the deprecated Siddhi extensions
     */
    private static void loadLocalExtensions(Map<String, Class> siddhiExtensionsMap,
                                            ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap,
                                            Map<String, Class> deprecatedSiddhiExtensionsMap) {
        Iterable<Class<?>> extensions = ClassIndex.getAnnotated(Extension.class);
        for (Class extension : extensions) {
            addExtensionToMap(extension, siddhiExtensionsMap, extensionHolderMap, deprecatedSiddhiExtensionsMap);
        }

        // load extensions related to incremental aggregation
        addExtensionToMap("incrementalAggregator:startTimeEndTime",
                IncrementalStartTimeEndTimeFunctionExecutor.class, siddhiExtensionsMap);
        addExtensionToMap("incrementalAggregator:timestampInMilliseconds",
                IncrementalUnixTimeFunctionExecutor.class, siddhiExtensionsMap);
        addExtensionToMap("incrementalAggregator:getTimeZone",
                IncrementalTimeGetTimeZone.class, siddhiExtensionsMap);
        addExtensionToMap("incrementalAggregator:getAggregationStartTime",
                IncrementalAggregateBaseTimeFunctionExecutor.class, siddhiExtensionsMap);
        addExtensionToMap("incrementalAggregator:shouldUpdate",
                IncrementalShouldUpdateFunctionExecutor.class, siddhiExtensionsMap);
    }

    /**
     * Adding extensions to Siddhi siddhiExtensionsMap.
     *
     * @param extensionClass                extension class
     * @param siddhiExtensionsMap           reference map for the Siddhi extension
     * @param extensionHolderMap            reference map for the Siddhi extension holder
     * @param deprecatedSiddhiExtensionsMap reference map for the deprecated Siddhi extensions
     */
    private static void addExtensionToMap(Class extensionClass, Map<String, Class> siddhiExtensionsMap,
                                          ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap,
                                          Map<String, Class> deprecatedSiddhiExtensionsMap) {
        Extension siddhiExtensionAnnotation = (Extension) extensionClass.getAnnotation(Extension.class);
        if (siddhiExtensionAnnotation != null) {
            if (!siddhiExtensionAnnotation.name().isEmpty()) {
                Class previousClass = null;
                if (!siddhiExtensionAnnotation.namespace().isEmpty()) {
                    String key = siddhiExtensionAnnotation.namespace() + SiddhiConstants.EXTENSION_SEPARATOR +
                            siddhiExtensionAnnotation.name();
                    Class existingValue = siddhiExtensionsMap.get(key);
                    if (existingValue == null) {
                        previousClass = siddhiExtensionsMap.put(key, extensionClass);
                        if (siddhiExtensionAnnotation.deprecated()) {
                            deprecatedSiddhiExtensionsMap.put(key, extensionClass);
                        }
                        for (Class clazz : extensionNameSpaceList) {
                            putToExtensionHolderMap(clazz, extensionClass, key,
                                    extensionHolderMap);
                        }
                    }
                    if (previousClass != null) {
                        log.warn("Dropping extension '" + previousClass + "' as '" + extensionClass + "' is " +
                                "loaded with the same namespace and name '" +
                                siddhiExtensionAnnotation.namespace() + SiddhiConstants.EXTENSION_SEPARATOR +
                                siddhiExtensionAnnotation.name() + "'");
                    }
                } else {
                    previousClass = siddhiExtensionsMap.put(siddhiExtensionAnnotation.name(), extensionClass);
                    if (siddhiExtensionAnnotation.deprecated()) {
                        deprecatedSiddhiExtensionsMap.put(siddhiExtensionAnnotation.name(), extensionClass);
                    }
                    if (previousClass != null) {
                        log.warn("Dropping extension '" + previousClass + "' as '" + extensionClass + "' is " +
                                "loaded with the same name '" + siddhiExtensionAnnotation.name() + "'");
                    }
                }
            } else {
                log.error("Unable to load extension " + extensionClass.getName() + ", missing Extension annotation.");
            }
        } else {
            log.error("Unable to load extension " + extensionClass.getName() + ", empty name element given in " +
                    "Extension annotation.");
        }
    }

    /**
     * Adding extensions to Siddhi siddhiExtensionsHolderMap.
     *
     * @param extensionSuperClass              extension super class (eg:Source)
     * @param extension                        extension class (eg:HttpSource)
     * @param extensionKey                     fully qualified extension name (namespace:extensionName)
     * @param extensionHolderConcurrentHashMap reference map for the Siddhi extension holder
     */
    private static void putToExtensionHolderMap(
            Class extensionSuperClass, Class extension, String extensionKey,
            ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderConcurrentHashMap) {
        if (extensionSuperClass.isAssignableFrom(extension)) {
            AbstractExtensionHolder extensionHolder = extensionHolderConcurrentHashMap.get(extensionSuperClass);
            if (extensionHolder != null) {
                if (extensionHolder.getExtension(extensionKey) != null) {
                    log.error("Extension class " + extension.getName() + " not loaded, as there is already an" +
                            " matching extension '" + extensionKey + "' implemented as " + extensionHolder
                            .getExtension(extensionKey).getName());
                } else {
                    extensionHolder.addExtension(extensionKey, extension);
                }
            }
        }
    }

    /**
     * Remove extensions to Siddhi siddhiExtensionsHolderMap.
     *
     * @param extensionKey                     fully qualified extension name (namespace:extensionName)
     * @param extension                        extension class (eg:HttpSource)
     * @param extensionHolderConcurrentHashMap reference map for the Siddhi extension holder
     */
    private static void removeFromExtensionHolderMap(
            String extensionKey,
            Class extension,
            ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderConcurrentHashMap) {
        for (Class extensionSuperClass : extensionNameSpaceList) {
            if (extensionSuperClass.isAssignableFrom(extension)) {
                AbstractExtensionHolder extensionHolder = extensionHolderConcurrentHashMap.get(extensionSuperClass);
                if (extensionHolder != null) {
                    extensionHolder.removeExtension(extensionKey);
                }
            }
        }
    }

    /**
     * Adding extensions to Siddhi siddhiExtensionsMap.
     *
     * @param fqExtensionName     fully qualified extension name (namespace:extensionName or extensionName)
     * @param extensionClass      extension class
     * @param siddhiExtensionsMap reference map for the Siddhi extension
     */
    private static void addExtensionToMap(String fqExtensionName, Class extensionClass,
                                          Map<String, Class> siddhiExtensionsMap) {
        Class previousClass = null;
        Class existingValue = siddhiExtensionsMap.get(fqExtensionName);
        if (existingValue == null) {
            previousClass = siddhiExtensionsMap.put(fqExtensionName, extensionClass);
        }
        if (previousClass != null) {
            log.warn("Dropping extension '" + extensionClass + "' as '" + previousClass + "' was already " +
                    "loaded with the same namespace and name '" + fqExtensionName + "'");
        }
    }

    /**
     * Class to listen to Bundle changes to update available extensions.
     */
    private static class ExtensionBundleListener implements BundleListener {

        private Map<Class, Integer> bundleExtensions = new HashMap<Class, Integer>();
        private Map<String, Class> siddhiExtensionsMap;
        private Map<String, Class> deprecatedSiddhiExtensionsMap;
        private ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderConcurrentHashMap;

        ExtensionBundleListener(Map<String, Class> siddhiExtensionsMap,
                                ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderConcurrentHashMap,
                                Map<String, Class> deprecatedSiddhiExtensionsMap) {
            this.siddhiExtensionsMap = siddhiExtensionsMap;
            this.extensionHolderConcurrentHashMap = extensionHolderConcurrentHashMap;
            this.deprecatedSiddhiExtensionsMap = deprecatedSiddhiExtensionsMap;
        }

        @Override
        public void bundleChanged(BundleEvent bundleEvent) {
            if (bundleEvent.getType() == BundleEvent.STARTED) {
                addExtensions(bundleEvent.getBundle());
            } else {
                removeExtensions(bundleEvent.getBundle());
            }
        }

        private void addExtensions(Bundle bundle) {
            ClassLoader classLoader = bundle.adapt(BundleWiring.class).getClassLoader();
            Iterable<Class<?>> extensions = ClassIndex.getAnnotated(Extension.class, classLoader);
            for (Class extension : extensions) {
                addExtensionToMap(extension, siddhiExtensionsMap,
                        extensionHolderConcurrentHashMap, deprecatedSiddhiExtensionsMap);
                bundleExtensions.put(extension, (int) bundle.getBundleId());
            }
        }

        private void removeExtensions(Bundle bundle) {
            bundleExtensions.entrySet().stream().filter(entry -> entry.getValue() ==
                    bundle.getBundleId()).forEachOrdered(entry -> {
                siddhiExtensionsMap.remove(entry.getKey());
            });
            bundleExtensions.entrySet().removeIf(entry -> entry.getValue() ==
                    bundle.getBundleId());
            BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
            if (bundleWiring != null) {
                ClassLoader classLoader = bundleWiring.getClassLoader();
                Iterable<Class<?>> extensions = ClassIndex.getAnnotated(Extension.class, classLoader);
                for (Class extension : extensions) {
                    Extension siddhiExtensionAnnotation = (Extension) extension.getAnnotation(Extension.class);
                    if (!siddhiExtensionAnnotation.namespace().isEmpty()) {
                        String key = siddhiExtensionAnnotation.namespace() + SiddhiConstants.EXTENSION_SEPARATOR +
                                siddhiExtensionAnnotation.name();
                        removeFromExtensionHolderMap(key, extension, extensionHolderConcurrentHashMap);
                    }
                }
            }
        }

        void loadAllExtensions(BundleContext bundleContext) {
            for (Bundle b : bundleContext.getBundles()) {
                if (b.getState() == Bundle.ACTIVE) {
                    addExtensions(b);
                }
            }
        }
    }
}
