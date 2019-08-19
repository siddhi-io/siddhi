/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.util.config;

import io.siddhi.core.exception.YAMLConfigManagerException;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.model.Extension;
import io.siddhi.core.util.config.model.ExtensionChildConfiguration;
import io.siddhi.core.util.config.model.Reference;
import io.siddhi.core.util.config.model.ReferenceChildConfiguration;
import io.siddhi.core.util.config.model.RootConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * YAML file based Config Manger
 */
public class YAMLConfigManager implements ConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(YAMLConfigManager.class);
    private RootConfiguration rootConfiguration;

    public YAMLConfigManager(String yamlContent) {
        init(yamlContent);
    }

    /**
     * Initialises YAML Config Manager by parsing the YAML file
     *
     * @throws YAMLConfigManagerException Exception is thrown if there are issues in processing thr yaml file
     */
    private void init(String yamlContent) throws YAMLConfigManagerException {
        try {
            CustomClassLoaderConstructor constructor = new CustomClassLoaderConstructor(
                    RootConfiguration.class, RootConfiguration.class.getClassLoader());
            PropertyUtils propertyUtils = new PropertyUtils();
            propertyUtils.setSkipMissingProperties(true);
            constructor.setPropertyUtils(propertyUtils);

            Yaml yaml = new Yaml(constructor);
            yaml.setBeanAccess(BeanAccess.FIELD);
            this.rootConfiguration = yaml.load(yamlContent);
        } catch (Exception e) {
            throw new YAMLConfigManagerException("Unable to parse YAML string, '" + yamlContent + "'.", e);
        }
    }

    @Override
    public ConfigReader generateConfigReader(String namespace, String name) {
        for (Extension extension : this.rootConfiguration.getExtensions()) {
            ExtensionChildConfiguration childConfiguration = extension.getExtension();
            if (childConfiguration.getNamespace().equals(namespace) &&
                    childConfiguration.getName().equals(name)) {
                return new YAMLConfigReader(childConfiguration.getProperties());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Could not find a matching configuration for name: " + name + "and namespace: " +
                    namespace + "!");
        }
        return new YAMLConfigReader(new HashMap<>());
    }

    @Override
    public Map<String, String> extractSystemConfigs(String name) {
        for (Reference reference : this.rootConfiguration.getRefs()) {
            ReferenceChildConfiguration childConf = reference.getReference();
            if (childConf.getName().equals(name)) {
                Map<String, String> referenceConfigs = new HashMap<>();
                referenceConfigs.put(SiddhiConstants.ANNOTATION_ELEMENT_TYPE, childConf.getType());
                referenceConfigs.putAll(childConf.getProperties());
                return referenceConfigs;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Could not find a matching reference for name: '" + name + "'!");
        }
        return new HashMap<>();
    }

    @Override
    public String extractProperty(String name) {
        String property = this.rootConfiguration.getProperties().get(name);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Could not find a matching configuration for property name: " + name + "");
        }
        return property;
    }
}
