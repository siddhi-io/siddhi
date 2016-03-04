/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.eventtable.sync;


import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.extension.eventtable.SyncEventTable;
import org.wso2.siddhi.extension.eventtable.exception.ThrottleConfigurationException;
import org.wso2.siddhi.extension.eventtable.util.ThrottleConstants;
import org.wso2.siddhi.query.api.definition.TableDefinition;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class which act as layer between the database and Siddhi. This class performs all the RDBMS related operations and Blooms Filter
 */
public class SyncTableHandler {

    private CountingBloomFilter[] bloomFilters;
    private boolean isBloomFilterEnabled;
    private int bloomFilterSize;
    private int bloomFilterHashFunction;
    private static final Logger log = Logger.getLogger(SyncTableHandler.class);
    private HttpClient httpClient;
    private TableDefinition tableDefinition;


    public SyncTableHandler(TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;

    }

    public void setBloomFilterProperties(int bloomFilterSize, int bloomFilterHashFunction) {
        this.bloomFilterSize = bloomFilterSize;
        this.bloomFilterHashFunction = bloomFilterHashFunction;
    }

    public boolean isBloomFilterEnabled() {
        return isBloomFilterEnabled;
    }

    public CountingBloomFilter[] getBloomFilters() {
        return bloomFilters;
    }

    //Bloom Filter Creation Logic
    public void buildBloomFilters(SyncEventTable syncEventTable) {

        SortedMap<Object, StreamEvent> remoteDataMap = new TreeMap<Object, StreamEvent>();
        //TODO Hack - restrict to size 1
        CountingBloomFilter[] bloomFilters = new CountingBloomFilter[1];

        for (int i = 0; i < bloomFilters.length; i++) {
            bloomFilters[i] = new CountingBloomFilter(bloomFilterSize, bloomFilterHashFunction, Hash.MURMUR_HASH);
        }

        String[] throttleKeyArray = retrieveThrottlingData();
        if (throttleKeyArray != null && throttleKeyArray.length > 0) {
            for (String throttleKey : throttleKeyArray) {
                StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                streamEvent.setOutputData(new Object[]{throttleKey});
                remoteDataMap.put(throttleKey, streamEvent);

                for (CountingBloomFilter bloomFilter : bloomFilters) {
                    bloomFilter.add(new Key(throttleKey.getBytes()));
                }
            }
        }
        this.bloomFilters = bloomFilters;
        this.isBloomFilterEnabled = true;
        syncEventTable.setInMemoryEventMap(remoteDataMap);
    }

//    private List<ThrottleObject> retrieveThrottlingData() {
//        String url = "http://localhost:9763/throttle/data/v1/throttle";
//        try {
//            HttpGet method = new HttpGet(url);
//            httpClient = new DefaultHttpClient();
//            HttpResponse httpResponse = httpClient.execute(method);
//            if (httpClient == null) {
//                httpClient = new SystemDefaultHttpClient();
//            }
//
//            String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
//            ObjectMapper mapper = new ObjectMapper();
//            return Arrays.asList(mapper.readValue(responseString, ThrottleObject[].class));
//
//        } catch (IOException e) {
//            log.error("Exception when retrieving throttling data from remote endpoint ", e);
//        }
//
//        return null;
//
//    }

    private String[] retrieveThrottlingData() {
//        String url = "http://10.100.1.65:9763/throttle/data/v1/throttleAsString";

        try {
            GlobalThrottleEngineConfig globalThrottleEngineConfig = loadCEPConfig();
            String url = "http://" + globalThrottleEngineConfig.getHostname() + ":" + globalThrottleEngineConfig.getHTTPPort() + "/throttle/data/v1/throttleAsString";

            HttpGet method = new HttpGet(url);
            httpClient = new DefaultHttpClient();
            HttpResponse httpResponse = httpClient.execute(method);
            if (httpClient == null) {
                httpClient = new SystemDefaultHttpClient();
            }

            String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            return responseString.split(",");

        } catch (IOException e) {
            log.error("Exception when retrieving throttling data from remote endpoint ", e);
        } catch (ThrottleConfigurationException e) {
            log.error("Exception while loading the throttling configuration ", e);
        }

        return null;

    }


    public static GlobalThrottleEngineConfig loadCEPConfig() throws ThrottleConfigurationException {
        String carbonHome = System.getProperty("carbon.config.dir.path");
        String path = carbonHome + File.separator + ThrottleConstants.CEP_CONFIG_XML;
        OMElement configElement = loadConfigXML(path);

        OMElement hostNameElement;
        OMElement tcpPortElement;
        OMElement sslPortElement;
        OMElement httpPortElement;
        OMElement httpsPortElement;
        OMElement usernameElement;
        OMElement passwordElement;
        OMElement streamNameElement;
        OMElement streamVersionElement;

        if ((hostNameElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.HOST_NAME))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no host name in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((tcpPortElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.TCP_PORT))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no TCP port in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((httpPortElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.HTTP_PORT))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no HTTP port in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((httpsPortElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.HTTPS_PORT))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no HTTPS port in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((sslPortElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.SSL_PORT))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no SSL port in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((usernameElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.USERNAME))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no username in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }
        if ((passwordElement = configElement.getFirstChildWithName(new QName(ThrottleConstants.PASSWORD))) == null) {
            throw new ThrottleConfigurationException("Invalid config element with no password in " +
                    ThrottleConstants.CEP_CONFIG_XML);
        }

        return new GlobalThrottleEngineConfig(hostNameElement.getText(), tcpPortElement.getText(), sslPortElement.getText(),
                httpPortElement.getText(), httpsPortElement.getText(), usernameElement.getText(), passwordElement.getText());
    }

    /**
     * Loads the configuration file in the given path as an OM element
     *
     * @return OMElement of config file
     * @throws ThrottleConfigurationException
     */
    private static OMElement loadConfigXML(String path) throws ThrottleConfigurationException {

        BufferedInputStream inputStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(new File(path)));
            XMLStreamReader parser = XMLInputFactory.newInstance().
                    createXMLStreamReader(inputStream);
            StAXOMBuilder builder = new StAXOMBuilder(parser);
            OMElement omElement = builder.getDocumentElement();
            omElement.build();
            return omElement;
        } catch (FileNotFoundException e) {
            throw new ThrottleConfigurationException("Configuration file cannot be found in the path : " + path, e);
        } catch (XMLStreamException e) {
            throw new ThrottleConfigurationException("Invalid XML syntax for configuration file located in the path :" + path, e);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                log.error("Can not shutdown the input stream", e);
            }
        }
    }


    public void addToBloomFilters(String key) {
        bloomFilters[0].add(new Key(key.getBytes()));
    }

    public void removeFromBloomFilters(String key) {
        try {
            bloomFilters[0].delete(new Key(key.getBytes()));
        } catch (Exception e) {
            if (e.getMessage().equals("Key is not a member")) {
                log.debug("Silently ignoring key not available in Bloom Filter case");
            } else {
                log.error("Exception when deleting a key from Bloom Filter");
            }
        }
    }

}
