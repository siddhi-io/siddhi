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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The ExtensionDocCache class works as the main manipulator of underline
 * API response cache.
 */
public class ExtensionDocCache {

    /**
     * The description key in the cache.
     */
    private static final String KEY_DESCRIPTION = "description";

    /**
     * The lastModifiedDateTime key in the cache.
     */
    private static final String KEY_LAST_MODIFIED_DATETIME = "lastModifiedDateTime";

    /**
     * The actual JSON instance of the cache.
     */
    private final JSONObject cache;

    /**
     * A memento of the cache instance.
     */
    private final JSONObject memento;

    /**
     * The path to the cache in local storage.
     */
    private final Path cachePath;

    /**
     * Whether cache is in-memory or not.
     */
    private boolean inMemory = false;

    /**
     * Constructs a ExtensionDocCache instance. Any failure occurs while reading
     * the cache from local storage will cause constructor to create an in-memory cache.
     *
     * @param cachePath the path to the cache
     */
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

    /**
     * Return whether extension exists in the cache.
     *
     * @param extension the name of the cache
     * @return whether extension exists in the cache
     */
    public boolean has(String extension) {
        return cache.has(extension);
    }

    /**
     * The add method adds an extension with description and last modified date to the cache.
     *
     * @param extension the name of the extension
     * @param description the description for the extension
     * @param lastModifiedDateTime the last modified date of the extension doc
     */
    public void add(String extension, String description, String lastModifiedDateTime) {
        JSONObject values = (cache.has(extension)) ? cache.getJSONObject(extension) : new JSONObject();

        values.put(KEY_DESCRIPTION, description);
        values.put(KEY_LAST_MODIFIED_DATETIME, lastModifiedDateTime);

        cache.put(extension, values);
    }

    /**
     * The getLastModifiedDateTime returns the last modified date
     * a given extension.
     *
     * @param extension the name of the extension
     * @return the the last modified date a given extension
     */
    public String getLastModifiedDateTime(String extension) {
        if (cache.has(extension)) {
            JSONObject values = cache.getJSONObject(extension);

            return values.getString(KEY_LAST_MODIFIED_DATETIME);
        }
        return null;
    }

    /**
     * Remove an extension from the cache.
     *
     * @param extension the name of the extension
     */
    public void remove(String extension) {
        cache.remove(extension);
    }

    /**
     * Remove relative complement of the given extension set
     * from the extension set in the cache.
     *
     * @param extensions the extension set
     */
    public void removeComplementOf(Set<String> extensions) {
        cache.keySet().removeIf(e -> !extensions.contains(e));
    }

    /**
     * @return whether cache is in-memory
     */
    public boolean isInMemory() {
        return inMemory;
    }

    /**
     * The commit method writes the data to the given location in the
     * local storage.
     *
     * 1). If there is no update done to the {@link ExtensionDocCache#cache}
     * it will return the false.
     * 3). Due to any IO error if writing fails it will return the false.
     *
     * Overall method only returns true if it has written to the local storage.
     *
     * @return whether changes has made to the underline local storage
     */
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

    /**
     * The getExtensionDescriptionMap method returns a map of extensions
     * with extension name as key and extension description as value.
     *
     * @return a map of extensions with extension name as key and extension description as value.
     */
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
