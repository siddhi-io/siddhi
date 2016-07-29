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

package org.wso2.siddhi.extension.sentimentanalysis;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);
    private static String method;

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
        if (attributeExpressionExecutors.length > 2) {
            throw new IllegalArgumentException(
                    "Invalid no of arguments passed to SentimentRate:getSentiment() function, "
                            + "required less than 3, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors.length == 2){
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new IllegalArgumentException("Required either  \"affin\" or \"stanford\" as second argument");
            } else {
                method = attributeExpressionExecutors[1].execute(null).toString().toLowerCase();
            }
        }
    }

    @Override
    protected Object execute(Object[] data) {
        String text = data[0].toString().toLowerCase();
        if ("affin".equals(method)) {
            return getAffinSentimentRate(text);
        } else if ("stanford".equals(method)) {
            return getStanfordSentimentRate(text);
        } else {
            LOGGER.error("Invalid option of SentimentRate. Option can be only \"affin\" or \"stanford\"");
            return null;
        }
    }

    @Override
    protected Object execute(Object data) {
        String text = data.toString().toLowerCase();
        return getStanfordSentimentRate(text);
    }

    private int getAffinSentimentRate(String text) {
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
                Matcher m = Pattern.compile("\\b" + word + "\\b").matcher(text);
                while (m.find()) {
                    rank += val;
                }
            }
        }
        return rank;
    }

    private int getStanfordSentimentRate(String text) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int totalRate = 0;
        String[] linesArr = text.split("\\.");
        for (int i = 0; i < linesArr.length; i++) {
            if (linesArr[i] != null) {
                Annotation annotation = pipeline.process(linesArr[i]);
                for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int score = RNNCoreAnnotations.getPredictedClass(tree);
                    totalRate = totalRate + (score - 2);
                }
            }
        }

        return totalRate;
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