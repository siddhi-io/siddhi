/*
 *  Copyright (c) 2016 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.wso2.siddhi.extension.sentiment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    public void start() {
        // Nothing to do here
    }

    @Override
    public void stop() {
        // Nothing to do here
    }

    @Override
    public Object[] currentState() {
        // No need to maintain a state.
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
        // No need to maintain a state
    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid no of arguments passed to sentiment:getRate() function, "
                            + "required 1, but found " + attributeExpressionExecutors.length);
        }
    }

    @Override
    protected Object execute(Object[] data) {
        return null;
    }

    @Override
    protected Object execute(Object data) {
        String[] affinWordBucket = null;
        try {
            affinWordBucket = getWordsBuckets("affinwords.txt");
        } catch (IOException e) {
            LOGGER.error("Failed to load affinwords.txt file ");
        }
        int rank = 0;
        String[] split;
        if (affinWordBucket != null){
            for (int i = 0; i < affinWordBucket.length; i++) {
                split = affinWordBucket[i].split(" ");
                String word = split[0].trim();
                int val = Integer.parseInt(split[split.length-1].replaceAll("\\s+", " ").trim());
                Matcher m = Pattern.compile("\\b" + word + "\\b").matcher(data.toString().toLowerCase());
                while (m.find()) {
                    rank += val;
                }
            }
        }
        return rank;
    }

    private String[] getWordsBuckets(String fileName) throws IOException {
        StringBuilder textChunk = new StringBuilder();
        try {
            InputStream in = getClass().getResourceAsStream("/" + fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line;
            while ((line = reader.readLine()) != null) {
                textChunk.append(line).append("\n");
            }
            in.close();
        } catch (Exception ex) {
            LOGGER.error("Error Reading " + fileName);
        }
        return textChunk.toString().split(",");
    }
}