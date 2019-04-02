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

import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

/**
 * The ExtensionDocStore class works as a storage to hold all the descriptions related to Siddhi
 * extensions.
 */
public class ExtensionDocStore {

    private static final Log LOG = new SystemStreamLog();

    /**
     * A Properties instance to hold the data.
     */
    private final Properties store;

    /**
     * A copy of {@link ExtensionDocStore#store}.
     */
    private final Properties memento;

    /**
     * Storage location of {@link ExtensionDocStore#store}.
     * This could be null if storage is in-memory.
     */
    private Path storePath;

    private FileTime storeLastModified;

    /**
     * Constructs a new ExtensionDocStore with an in-memory storage.
     */
    public ExtensionDocStore() {
        this.store = new Properties() {
            @Override
            public synchronized Enumeration<Object> keys() {
                return Collections.enumeration(new TreeSet<>(super.keySet()));
            }
        };
        storePath = null;

        memento = new Properties();

        storeLastModified = FileTime.fromMillis(0);
    }

    /**
     * Constructs a new ExtensionDocStore with a local storage.
     * Any failure occurs while accessing the local storage will
     * cause constructor to create a new ExtensionDocStore with
     * an in-memory storage.
     *
     * @param storePath the path to the local storage
     */
    public ExtensionDocStore(Path storePath) {
        this();
        this.storePath = storePath;

        try (FileInputStream stream = new FileInputStream(storePath.toString())) {
            store.load(stream);
            memento.putAll(store);

            storeLastModified = Files.getLastModifiedTime(storePath);
        } catch (IOException ignored) {
            storeLastModified = FileTime.fromMillis(0);
        }
    }

    /**
     * Return whether it contains the given extension.
     *
     * @param extension the name of the extension
     * @return whether it contains the given extension
     */
    public boolean has(String extension) {
        return store.getProperty(extension) != null;
    }

    /**
     * The add method adds a description to a given extension.
     * If extension does not exist it will get created otherwise
     * it overrides the existing value.
     *
     * @param extension the name of the extension
     * @param description the description to be added
     */
    public void add(String extension, String description) {
        store.setProperty(extension, description);
    }

    /**
     * The remove method removes a given extension.
     *
     * @param extension the name of the extension
     */
    public void remove(String extension) {
        store.remove(extension);
    }

    /**
     * The commit method writes the data to the given location in the
     * local storage.
     *
     * 1). If ExtensionDocStore instance has only an in-memory
     * storage it will simply ignore the process and return the false.
     * 2). If there is no update done to the {@link ExtensionDocStore#store}
     * it will return the false.
     * 3). Due to any IO error if writing fails it will return the false.
     *
     * Overall method only returns true if it has written to the local storage.
     *
     * @return whether changes has made to the underline local storage
     */
    public boolean commit() {
        if (storePath == null) {
            return false;
        }
        if (memento.equals(store)) {
            return false;
        }
        try (FileOutputStream outputStream = new FileOutputStream(storePath.toString())) {
            store.store(outputStream, null);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * The getLastModified method returns the last modified datetime
     * in the file in {@link ExtensionDocStore#storePath}.
     *
     * @return the last modified datetime of the local storage.
     */
    public FileTime getLastModified() {
        return storeLastModified;
    }

    /**
     * Return the data set as a Map.
     *
     * @return the data set as a Map
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, String> asMap() {
        return (Map) store;
    }
}
