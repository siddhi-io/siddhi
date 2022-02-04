/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.aggregation.persistedaggregation.config;

import com.google.common.collect.Maps;
import io.siddhi.core.exception.CannotLoadConfigurationException;
import io.siddhi.core.util.parser.AggregationParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import static io.siddhi.core.util.SiddhiConstants.DB_AGGREGATION_CONFIG_FILE;

/**
 * Util class for reading persisted aggregation queries
 **/
public class DBAggregationQueryUtil {

    private static final Logger LOG = LogManager.getLogger(DBAggregationQueryUtil.class);
    private static DBAggregationConfigurationMapper mapper;

    /**
     * Isolates a particular RDBMS query configuration entry which matches the retrieved DB metadata.
     *
     * @param databaseType the database type against which the entry should be matched.
     * @return the matching RDBMS query configuration entry.
     * @throws CannotLoadConfigurationException if the configuration cannot be loaded.
     */
    public static DBAggregationQueryConfigurationEntry lookupCurrentQueryConfigurationEntry(
            AggregationParser.Database databaseType) throws CannotLoadConfigurationException {
        DBAggregationConfigurationMapper mapper = loadDBAggregationConfigurationMapper();
        DBAggregationQueryConfigurationEntry entry = mapper.lookupEntry(databaseType.toString());
        if (entry != null) {
            return entry;
        } else {
            throw new CannotLoadConfigurationException("Cannot find a database section in the RDBMS "
                    + "configuration for the database: " + databaseType);
        }
    }

    /**
     * Checks and returns an instance of the RDBMS query configuration mapper.
     *
     * @return an instance of {@link DBAggregationConfigurationMapper}.
     * @throws CannotLoadConfigurationException if the configuration cannot be loaded.
     */
    private static DBAggregationConfigurationMapper loadDBAggregationConfigurationMapper()
            throws CannotLoadConfigurationException {
        if (mapper == null) {
            synchronized (DBAggregationQueryUtil.class) {
                DBAggregationQueryConfiguration config = loadQueryConfiguration();
                mapper = new DBAggregationConfigurationMapper(config);
            }
        }
        return mapper;
    }

    private static DBAggregationQueryConfiguration loadQueryConfiguration() throws CannotLoadConfigurationException {
        return new DBAggregationConfigLoader().loadConfiguration();
    }

    /**
     * RDBMS configuration mapping class to be used to lookup matching configuration entry with a data source.
     */
    private static class DBAggregationConfigurationMapper {

        private List<Map.Entry<Pattern, DBAggregationQueryConfigurationEntry>> entries = new ArrayList<>();

        public DBAggregationConfigurationMapper(DBAggregationQueryConfiguration config) {
            for (DBAggregationQueryConfigurationEntry entry : config.getDatabases()) {
                this.entries.add(Maps.immutableEntry(Pattern.compile(
                        entry.getDatabaseName().toLowerCase(Locale.ENGLISH)), entry));
            }
        }

        private List<DBAggregationQueryConfigurationEntry> extractMatchingConfigEntries(String dbName) {
            List<DBAggregationQueryConfigurationEntry> result = new ArrayList<>();
            this.entries.forEach(entry -> {
                if (entry.getKey().matcher(dbName).find()) {
                    result.add(entry.getValue());
                }
            });
            return result;
        }

        public DBAggregationQueryConfigurationEntry lookupEntry(String dbName) {
            List<DBAggregationQueryConfigurationEntry> dbResults = this.extractMatchingConfigEntries(
                    dbName.toLowerCase(Locale.ENGLISH));
            if (dbResults.isEmpty()) {
                return null;
            }
            return dbResults.get(0);
        }
    }

    /**
     * Child class with a method for loading the JAXB configuration mappings.
     */
    private static class DBAggregationConfigLoader {

        /**
         * Method for loading the configuration mappings.
         *
         * @return an instance of {@link DBAggregationQueryConfiguration}.
         * @throws CannotLoadConfigurationException if the config cannot be loaded.
         */
        private DBAggregationQueryConfiguration loadConfiguration() throws CannotLoadConfigurationException {
            InputStream inputStream = null;
            try {
                JAXBContext ctx = JAXBContext.newInstance(DBAggregationQueryConfiguration.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                ClassLoader classLoader = getClass().getClassLoader();
                inputStream = classLoader.getResourceAsStream(DB_AGGREGATION_CONFIG_FILE);
                if (inputStream == null) {
                    throw new CannotLoadConfigurationException(DB_AGGREGATION_CONFIG_FILE
                            + " is not found in the classpath");
                }
                return (DBAggregationQueryConfiguration) unmarshaller.unmarshal(inputStream);
            } catch (JAXBException e) {
                throw new CannotLoadConfigurationException(
                        "Error in processing RDBMS query configuration: " + e.getMessage(), e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        LOG.error(String.format("Failed to close the input stream for %s", DB_AGGREGATION_CONFIG_FILE));
                    }
                }
            }
        }
    }
}
