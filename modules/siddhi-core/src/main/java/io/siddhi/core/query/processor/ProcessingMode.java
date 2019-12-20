/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.query.processor;

/**
 * The processing modes supported by Stream Processors
 */
public enum ProcessingMode {
    BATCH,
    SLIDE,
    HOP,
    GROUP,
    RESET;

    public static ProcessingMode findUpdatedProcessingMode(
            ProcessingMode currentProcessingMode,
            ProcessingMode updatingProcessingMode) {
        ProcessingMode overallProcessingMode = currentProcessingMode;
        switch (updatingProcessingMode) {
            case BATCH:
                break;
            case RESET:
                switch (overallProcessingMode) {
                    case SLIDE:
                        break;
                    case HOP:
                        break;
                    case GROUP:
                        break;
                    case RESET:
                    case BATCH:
                        overallProcessingMode = ProcessingMode.RESET;
                        break;
                }
                break;
            case HOP:
                switch (overallProcessingMode) {
                    case HOP:
                        break;
                    case BATCH:
                    case RESET:
                        overallProcessingMode = ProcessingMode.HOP;
                        break;
                    case SLIDE:
                    case GROUP:
                        overallProcessingMode = ProcessingMode.SLIDE;
                        break;
                }
                break;
            case GROUP:
                switch (overallProcessingMode) {
                    case GROUP:
                        break;
                    case BATCH:
                    case RESET:
                        overallProcessingMode = ProcessingMode.GROUP;
                        break;
                    case SLIDE:
                    case HOP:
                        overallProcessingMode = ProcessingMode.SLIDE;
                        break;
                }
                break;
            case SLIDE:
                overallProcessingMode = ProcessingMode.SLIDE;
                break;
        }
        return overallProcessingMode;
    }
}
