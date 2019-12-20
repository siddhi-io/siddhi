/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.service.impl;

import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.util.AnnotationHelper;
import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.service.api.ApiResponseMessage;
import io.siddhi.service.api.NotFoundException;
import io.siddhi.service.api.SiddhiApiService;
import io.siddhi.service.util.SiddhiAppConfiguration;
import io.siddhi.service.util.SiddhiServiceConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.core.Response;

/**
 * Siddhi Service Implementataion Class
 */
public class SiddhiApiServiceImpl extends SiddhiApiService {

    private Log log = LogFactory.getLog(SiddhiApiServiceImpl.class);
    private SiddhiManager siddhiManager = new SiddhiManager();
    private Map<String, Map<String, InputHandler>> siddhiAppSpecificInputHandlerMap = new ConcurrentHashMap<>();
    private Map<String, SiddhiAppConfiguration> siddhiAppConfigurationMap = new ConcurrentHashMap<>();
    private Map<String, SiddhiAppRuntime> siddhiAppRunTimeMap = new ConcurrentHashMap<>();

    @Override
    public Response siddhiArtifactDeployPost(String siddhiApp) throws NotFoundException {

        log.info("SiddhiApp = " + siddhiApp);
        String jsonString = new Gson().toString();
        try {
            SiddhiApp parsedSiddhiApp = SiddhiCompiler.parse(siddhiApp);
            String siddhiAppName = AnnotationHelper.getAnnotationElement(
                    SiddhiServiceConstants.ANNOTATION_NAME_NAME, null, parsedSiddhiApp.
                            getAnnotations()).getValue();
            if (!siddhiAppRunTimeMap.containsKey(siddhiApp)) {
                SiddhiAppConfiguration siddhiAppConfiguration = new SiddhiAppConfiguration();
                siddhiAppConfiguration.setName(siddhiAppName);
                siddhiAppConfigurationMap.put(siddhiAppName, siddhiAppConfiguration);

                SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

                if (siddhiAppRuntime != null) {
                    Set<String> streamNames = siddhiAppRuntime.getStreamDefinitionMap().keySet();
                    Map<String, InputHandler> inputHandlerMap = new ConcurrentHashMap<>(streamNames.size());

                    for (String streamName : streamNames) {
                        inputHandlerMap.put(streamName, siddhiAppRuntime.getInputHandler(streamName));
                    }

                    siddhiAppSpecificInputHandlerMap.put(siddhiAppName, inputHandlerMap);

                    siddhiAppRunTimeMap.put(siddhiAppName, siddhiAppRuntime);
                    siddhiAppRuntime.start();

                    jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.OK,
                            "Siddhi app is deployed " +
                                    "and runtime is created"));
                }
            } else {
                jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.ERROR,
                        "There is a Siddhi app already " +
                                "exists with same name"));
            }

        } catch (Exception e) {
            jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.ERROR, e.getMessage()));
        }

        return Response.ok()
                .entity(jsonString)
                .build();
    }

    @Override
    public Response siddhiArtifactUndeploySiddhiAppGet(String siddhiAppName) throws NotFoundException {

        String jsonString = new Gson().toString();
        if (siddhiAppName != null) {
            if (siddhiAppRunTimeMap.containsKey(siddhiAppName)) {
                siddhiAppRunTimeMap.remove(siddhiAppName);
                siddhiAppConfigurationMap.remove(siddhiAppName);
                siddhiAppSpecificInputHandlerMap.remove(siddhiAppName);

                jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.OK,
                        "Siddhi app removed successfully"));
            } else {
                jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.ERROR,
                        "There is no siddhi app exist " +
                                "with provided name : " + siddhiAppName));
            }
        } else {
            jsonString = new Gson().toJson(new ApiResponseMessage(ApiResponseMessage.ERROR,
                    "nvalid Request"));

        }
        return Response.ok()
                .entity(jsonString)
                .build();
    }
}
