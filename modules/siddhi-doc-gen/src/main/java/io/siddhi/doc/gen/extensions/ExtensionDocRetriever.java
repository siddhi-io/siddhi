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
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
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
            String httpStandardLastModified = getRfc1123StandardTime(store.getLastModified());

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

            store.commit();
        } catch (IOException | ReflectiveOperationException e) {
            return false;
        }
        return true;
    }

    /**
     * Return the RFC 1123 standard datetime.
     *
     * Note
     * ====
     * Following datetime formatter may replaceable by more robust solutions like
     * 'DateTimeFormatter.RFC_1123_DATE_TIME', while doing so will lead to some strange
     * behaviours. Above formatter format date of month in following way.
     *
     * Wed, 1 Aug 2018 14:56:46 GMT
     *
     * If you set conditional request headers using above format in a Github API request,
     * it will give you the correct result while silently reducing your request quota.
     * So the correct format Github expects is,
     *
     * Wed, 01 Aug 2018 14:56:46 GMT
     *
     * That's why a custom pattern is used in this case.
     *
     * @param time the time to be converted
     * @return the RFC 1123 standard datetime
     */
    private String getRfc1123StandardTime(FileTime time) {
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("EEE, dd MMM yyyy HH:mm:ss O")
                .withZone(ZoneOffset.UTC);

        return formatter.format(time.toInstant());
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
