/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
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
package io.siddhi.doc.gen.extensions;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ExtensionDocCache {

    private static final String SIDDHI_CREATED_DATETIME = "Mon, 21 Apr 2014 09:51:36 GMT";

    private static final String KEY_DESCRIPTION = "description";

    private static final String KEY_LAST_MODIFIED_DATETIME = "lastModifiedDateTime";

    private final JSONObject cache;

    private final JSONObject memento;

    private final Path cachePath;

    private boolean inMemory = false;

    public ExtensionDocCache(Path cachePath) {
        JSONObject cache;
        try {
            JSONTokener tokener = new JSONTokener(cachePath.toUri().toURL().openStream());
            cache = new JSONObject(tokener);
            if (cache.length() == 0) {
                inMemory = true;
            }
        } catch (IOException e) {
            inMemory = true;
            cache = new JSONObject();
        }
        this.cache = cache;
        this.cachePath = cachePath;
        memento = new JSONObject(cache.toString());
    }

    public boolean has(String extension) {
        return cache.has(extension);
    }

    public void add(String extension, String description, String lastModifiedDateTime) {
        JSONObject values = (cache.has(extension)) ? cache.getJSONObject(extension) : new JSONObject();

        values.put(KEY_DESCRIPTION, description);
        values.put(KEY_LAST_MODIFIED_DATETIME, lastModifiedDateTime);

        cache.put(extension, values);
    }

    public String getLastModifiedDateTime(String extension) {
        if (inMemory) {
            return SIDDHI_CREATED_DATETIME;
        }
        if (cache.has(extension)) {
            JSONObject values = cache.getJSONObject(extension);

            return values.getString(KEY_LAST_MODIFIED_DATETIME);
        }
        return null;
    }

    public void remove(String extension) {
        cache.remove(extension);
    }

    public void removeComplementOf(Set<String> extensions) {
        cache.keySet().removeIf(e -> !extensions.contains(e));
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public boolean commit() {
        if (cache.equals(memento)) {
            return false;
        }
        try {
            try (PrintWriter writer = new PrintWriter(cachePath.toString(), StandardCharsets.UTF_8.toString())) {
                writer.println(cache.toString(2));
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public Map<String, String> getExtensionDescriptionMap() {
        Map<String, String> map = new TreeMap<>();
        Set<String> extensions = cache.keySet();

        for (String extension : extensions) {
            JSONObject values = cache.getJSONObject(extension);
            map.put(extension, values.getString(KEY_DESCRIPTION));
        }

        return map;
    }
}
