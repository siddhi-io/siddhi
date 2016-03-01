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


import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.wso2.siddhi.extension.eventtable.util.ThrottleObject;
import org.wso2.siddhi.query.api.definition.TableDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
        if (throttleKeyArray != null) {
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
        String url = "http://10.100.1.65:9763/throttle/data/v1/throttleAsString";
        try {
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
        }

        return null;

    }

    public void addToBloomFilters(String key) {
        bloomFilters[0].add(new Key(key.getBytes()));
    }

    public void removeFromBloomFilters(String key) {
        bloomFilters[0].delete(new Key(key.getBytes()));
    }

}
