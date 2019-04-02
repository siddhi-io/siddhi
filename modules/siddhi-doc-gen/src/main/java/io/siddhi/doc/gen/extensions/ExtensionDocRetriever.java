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

/**
 * The ExtensionDocRetriever class provides ability to retrieve all the descriptions
 * for Siddhi extensions. It manages process to retrieve and save data from the remote
 * extension repositories.
 */
public class ExtensionDocRetriever {

    /**
     * A log to log various kind of state changes.
     */
    private static final Log LOG = new SystemStreamLog();

    private static final String CREDENTIALS = "credentials.properties";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";

    /**
     * The Github username which all extensions exist.
     */
    private final String baseGithubId;

    /**
     * The extension name list.
     */
    private final List<String> extensions;

    /**
     * A storage to keep track of descriptions fetched from the remote repositories.
     */
    private final ExtensionDocStore store;

    public ExtensionDocRetriever(String baseGithubId, List<String> extensions, Path cachePath) {

        this.baseGithubId = baseGithubId;
        this.extensions = extensions;

        store = new ExtensionDocStore(cachePath);
    }

    /**
     * The pull method retrieves corresponding descriptions from extension repositories.
     * The method only retrieves data if they have modified in the repository.
     *
     * @return whether it is a successful pull.
     */
    public boolean pull() {
        try {
            /* Last modified date of the extension cache storage in RFC 1123 standard
               format. example: Fri, 31 Dec 1999 23:59:59 GMT */
            String httpStandardLastModified = DateTimeFormatter.RFC_1123_DATE_TIME
                    .withZone(ZoneOffset.UTC)
                    .format(store.getLastModified().toInstant());

            int i = 0;
            for (; i < extensions.size(); i++) {
                final String extension = extensions.get(i);

                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseGithubId, extension)
                        .isReadme(true)
                        .build();
                if (store.has(extension)) {
                    /* Set HTTP header makes the request conditional. */
                    githubClient.setHeader("If-Modified-Since", httpStandardLastModified);
                }
                HtmlContentsResponse response = githubClient.getContentsResponse(HtmlContentsResponse.class);
                /* API throttling limit reached, could not process further without credentials */
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

                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseGithubId, extension)
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

    /**
     * !!!!!! WAINING !!!!!!
     * TODO: Should not package credential properties to jar.
     */
    private boolean loadCredentials(Properties credentials) {
        try (InputStream stream = ExtensionDocRetriever.class.getClassLoader().getResourceAsStream(CREDENTIALS)) {
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

    /**
     * Update the extension storage according to the API response.
     *
     * @param extension the name of the extension.
     * @param response the API response.
     * @throws IOException if error occurs while extracting the API response.
     */
    private void updateStore(String extension, ContentsResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
            case 200: {
                ContentReader reader = response.getContentReader();
                String firstParagraph = reader.getFirstParagraph();
                if (firstParagraph == null) {
                    return;
                }
                LOG.info(String.format(
                        "Request success for '%s' with status code '%d'", extension, status));
                store.add(extension, firstParagraph);
                break;
            }
            case 304:
                LOG.info(String.format(
                        "Request success for '%s' with status code '%d'", extension, status));
                break;
            case 404:
                store.remove(extension);
                break;
            default:
                LOG.info(String.format(
                        "Error while updating extension [%s]%n%s", extension, response.getError()));
        }
    }

    /**
     * Return the store data as a map.
     *
     * @return the store data as a map.
     */
    public Map<String, String> asMap() {
        return store.asMap();
    }
}
