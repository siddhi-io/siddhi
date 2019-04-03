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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The ExtensionDocRetriever class provides ability to retrieve all the descriptions
 * for Siddhi extensions. It manages process of retrieving and saving data from remote
 * extension repositories.
 */
public class ExtensionDocRetriever {

    /**
     * A log to log various kind of state changes.
     */
    private static final Log log = new SystemStreamLog();

    /**
     * The Github username where all extensions exist.
     */
    private final String baseGithubId;

    /**
     * The extension names list.
     */
    private final List<String> extensions;

    /**
     * A storage to keep track of descriptions fetched from the remote repositories.
     */
    private final ExtensionDocStore store;

    /**
     * Whether the API has reached it's limits.
     */
    private boolean throttled = false;

    public ExtensionDocRetriever(String baseGithubId, List<String> extensions, ExtensionDocStore store) {
        this.baseGithubId = baseGithubId;
        this.extensions = extensions;
        this.store = store;
    }

    /**
     * The pull method retrieves corresponding descriptions from extension repositories.
     * The method only retrieves data if they have been modified in the repository.
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

            log.info("Retrieving extension descriptions from remote repositories");

            for (final String extension : extensions) {
                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseGithubId, extension)
                        .isReadme(true)
                        .build();
                if (store.has(extension)) {
                    /* Sets HTTP header to make the request conditional */
                    githubClient.setHeader("If-Modified-Since", httpStandardLastModified);
                }
                HtmlContentsResponse response = githubClient.getContentsResponse(HtmlContentsResponse.class);
                /* API throttling limit reached, could not process further without credentials */
                if (response.getStatus() == 403) {
                    throttled = true;
                    break;
                }
                updateStore(extension, response);
            }

            removeComplementOfGivenExtensionsFromStore();

            boolean commit = store.commit();
            if (!store.isInMemory() && !commit) {
                log.debug("Error occurred while updating the doc cache.");
            }
        } catch (IOException | ReflectiveOperationException e) {
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
                store.add(extension, firstParagraph);
                log.debug(String.format("Request to '%s' succeed with status '%s'", extension, status));
                break;
            }
            case 304:
                log.debug(String.format("Request to '%s' succeed with status '%s'", extension, status));
                break;
            case 404:
                log.debug(String.format("Request to '%s' failed with status '%s'", extension, status));
                store.remove(extension);
                break;
            default:
                log.error(String.format("Error occurred while retrieving the extension '%s': %s",
                        extension, response.getError().toString()));
        }
    }

    /**
     * Remove the relative compliment of {@link ExtensionDocRetriever#extensions}
     * from {@link ExtensionDocRetriever#store}.
     */
    private void removeComplementOfGivenExtensionsFromStore() {
        Set<String> extensionSet = new TreeSet<>(extensions);
        Set<String> storeKeys = store.asMap().keySet();

        storeKeys.removeIf(e -> !extensionSet.contains(e));
    }

    /**
     * @return whether API has reached it's throttling limits.
     */
    public boolean isThrottled() {
        return throttled;
    }
}
