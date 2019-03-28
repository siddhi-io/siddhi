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

import io.siddhi.doc.gen.extensions.githubclient.ContentReader;
import io.siddhi.doc.gen.extensions.githubclient.ContentsResponse;
import io.siddhi.doc.gen.extensions.githubclient.GithubContentsClient;
import io.siddhi.doc.gen.extensions.githubclient.HtmlContentsResponse;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ExtensionDocRequester {

    private static final Log LOG = new SystemStreamLog();

    private static final String CREDENTIALS = "credentials.properties";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";

    private final String baseRepo;
    private final List<String> extensions;

    private final ExtensionDocStore store;

    public ExtensionDocRequester(String baseRepo, List<String> extensions, Path cachePath) {

        this.baseRepo = baseRepo;
        this.extensions = extensions;

        store = new ExtensionDocStore(cachePath);
    }

    public boolean pull() {
        try {
            String httpStandardLastModified = DateTimeFormatter.RFC_1123_DATE_TIME
                    .withZone(ZoneOffset.UTC)
                    .format(store.getLastModified().toInstant());

            int i = 0;
            for (; i < extensions.size(); i++) {
                final String extension = extensions.get(i);

                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseRepo, extension)
                        .isReadme(true)
                        .build();
                if (store.has(extension)) {
                    githubClient.setHeader("If-Modified-Since", httpStandardLastModified);
                }
                HtmlContentsResponse response = githubClient.getContentsResponse(HtmlContentsResponse.class);
                if (response.getStatus() == 403) {
                    break;
                }
                updateStore(extension, response);
            }
            Properties credentials = new Properties();
            if (!loadCredentials(credentials)) {
                return false;
            }

            for (; i < extensions.size(); i++) {
                final String extension = extensions.get(i);

                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseRepo, extension)
                        .isReadme(true)
                        .queryParam(CLIENT_ID, credentials.getProperty(CLIENT_ID))
                        .queryParam(CLIENT_SECRET, credentials.getProperty(CLIENT_SECRET))
                        .build();
                if (store.has(extension)) {
                    githubClient.setHeader("If-Modified-Since", httpStandardLastModified);
                }
                updateStore(extension, githubClient.getContentsResponse(HtmlContentsResponse.class));
            }

            return store.commit();
        } catch (IOException | ReflectiveOperationException e) {
            return false;
        }
    }

    private boolean loadCredentials(Properties credentials) {
        try (InputStream stream = ExtensionDocRequester.class.getClassLoader().getResourceAsStream(CREDENTIALS)) {
            if (stream == null) {
                return false;
            }
            credentials.load(stream);
            if (credentials.getProperty(CLIENT_ID) == null || credentials.getProperty(CLIENT_SECRET) == null) {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private void updateStore(String extension, ContentsResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
            case 200: {
                ContentReader reader = response.getContentReader();
                String firstParagraph = reader.getFirstParagraph();
                if (firstParagraph == null) {
                    return;
                }
                LOG.info("updating...");
                store.add(extension, firstParagraph);
                break;
            }
            case 304:
                LOG.info("not modified");
                break;
            case 404:
                store.remove(extension);
                break;
            default:
                LOG.info(response.getError().toString());
        }
    }

    public Map<String, String> asMap() {
        return store.asMap();
    }
}
