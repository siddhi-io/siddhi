/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.exception.YAMLConfigManagerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Util class to read file content
 */
public class FileReader {

    private static final Logger LOG = LogManager.getLogger(FileReader.class);

    public static String readYAMLConfigFile(Path filePath) throws YAMLConfigManagerException {
        if (!filePath.toFile().exists()) {
            throw new YAMLConfigManagerException("Error while initializing YAML config manager, " +
                    "YAML file does not exist with path '" + filePath.toAbsolutePath().toString() + "'.");
        }
        if (!filePath.toString().endsWith(".yaml")) {
            throw new YAMLConfigManagerException("Error while initializing YAML config manager, file extension " +
                    "'yaml' expected");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialize config provider instance from configuration file: " + filePath.toString());
        }
        try {
            return readContent(filePath);
        } catch (IOException e) {
            throw new YAMLConfigManagerException("Unable to read config file '" + filePath.toAbsolutePath().toString()
                    + "'.", e);
        }
    }

    private static String readContent(Path filePath) throws IOException {
        byte[] contentBytes = Files.readAllBytes(filePath);
        return new String(contentBytes, StandardCharsets.UTF_8);
    }

}
