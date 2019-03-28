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

public class ExtensionDocStore {

    private static final Log LOG = new SystemStreamLog();

    private final Properties store;
    private final Properties memento;

    private Path storePath;

    public ExtensionDocStore() {
        this.store = new Properties() {
            @Override
            public synchronized Enumeration<Object> keys() {
                return Collections.enumeration(new TreeSet<>(super.keySet()));
            }
        };
        storePath = null;

        memento = new Properties();
    }

    public ExtensionDocStore(Path storePath) {
        this();
        this.storePath = storePath;

        try (FileInputStream stream = new FileInputStream(storePath.toString())) {
            store.load(stream);
            memento.putAll(store);
        } catch (IOException e) {
        }
    }

    public boolean has(String extension) {
        return store.getProperty(extension) != null;
    }

    public void add(String extension, String doc) {
        store.setProperty(extension, doc);
    }

    public void remove(String extension) {
        store.remove(extension);
    }

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

    public FileTime getLastModified() {
        if (storePath == null) {
            return FileTime.fromMillis(0);
        }
        try {
            return Files.getLastModifiedTime(storePath);
        } catch (IOException e) {
            return FileTime.fromMillis(0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, String> asMap() {
        return (Map) store;
    }
}
