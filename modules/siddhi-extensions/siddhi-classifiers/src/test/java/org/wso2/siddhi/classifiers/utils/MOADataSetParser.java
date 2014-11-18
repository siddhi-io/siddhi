/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers.utils;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * This is the class which parse the MOA.arff file and generate the stream definition for Shiddhi, this will help to run
 * the data-sets used by MOA
 */
public class MOADataSetParser {
    static final Logger log = Logger.getLogger(MOADataSetParser.class);
    private static SiddhiConfiguration configuration;
    private static SiddhiManager siddhiManager;

    /**
     * This file should only have nominal attributes, if the data-set have numeric attributes, this can be converted to have
     * nominal attribute using weka GUI tool.
     * @param fileName dataset file which has .arff extension.
     * @return
     */
    public static String getStreamDefinition(String streamName,String fileName) {
        StringBuffer buffer = new StringBuffer();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
            }
            reader.close();
            String definition = buffer.toString().split("@data")[0];
            List<String> attributes = Arrays.asList(definition.substring(definition.indexOf("@attribute")).split("@attribute"));
            buffer.delete(0, buffer.length());
            buffer.append("define stream ").append(streamName).append("(");
            for(String attr:attributes){
                if(!attr.isEmpty()) {
                    String temp = attr;
                    attr = attr.replaceAll("\\{", "nominal\\(");
                    attr = attr.replaceAll("\\}", "\\)");
                    buffer.append(attr);
                    if (attributes.indexOf(temp) != attributes.size()-1) {
                        buffer.append(",");
                    }
                }
            }
            buffer.append(")");
            System.out.println(buffer.toString());
            //After readin the stream we try to create a dummy stream and validate the grammar
            configuration = new SiddhiConfiguration();
            configuration.setAsyncProcessing(false);
            siddhiManager = new SiddhiManager(configuration);
            try {
                siddhiManager.defineStream(buffer.toString());
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
                return null;
            }
            // validation is successful if no exception thrown during stream definition
        } catch (Exception e) {
            System.err.format("Exception occurred trying to read '%s'.", fileName);
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
        return buffer.toString();
    }

    public static void sendAllEvents(InputHandler loginSucceedEvents, String fileName) {
        StringBuffer buffer = new StringBuffer();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
                buffer.append("\n");
            }
            reader.close();
            String data = buffer.toString().split("@data")[1];
            List<String> tuples = Arrays.asList(data.split("\\r?\\n"));
            for (String tuple : tuples) {
                if(!tuple.isEmpty()) {
                    loginSucceedEvents.send(tuple.split(","));
                }
            }
        } catch (Exception e) {
            System.err.format("Exception occurred trying to read '%s'.", fileName);
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        File file = new File(".");
        file.getAbsoluteFile();
        MOADataSetParser.getStreamDefinition("airline", "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/airlines.arff");
        MOADataSetParser.getStreamDefinition("porker", "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/porker.arff");
    }

}
