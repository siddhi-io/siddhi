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

import io.siddhi.doc.gen.extensions.githubclient.ContentsBodyReader;
import io.siddhi.doc.gen.extensions.githubclient.ContentsResponse;
import io.siddhi.doc.gen.extensions.githubclient.GithubContentsClient;
import io.siddhi.doc.gen.extensions.githubclient.HtmlContentsResponse;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.IOException;
import java.nio.file.attribute.FileTime;
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

    private final ExtensionDocCache cache;

    /**
     * Whether the API has reached it's limits.
     */
    private boolean throttled = false;

    public ExtensionDocRetriever(String baseGithubId, List<String> extensions, ExtensionDocCache cache) {
        this.baseGithubId = baseGithubId;
        this.extensions = extensions;
        this.cache = cache;
    }

    /**
     * The pull method retrieves corresponding descriptions from extension repositories.
     * The method only retrieves data if they have been modified in the repository.
     *
     * @return whether it is a successful pull.
     */
    public boolean pull() {
        try {
            for (final String extension : extensions) {
                GithubContentsClient githubClient = new GithubContentsClient.Builder(baseGithubId, extension)
                        .isReadme(true)
                        .build();
                if (cache.has(extension)) {
                    /* Sets HTTP header to make the request conditional */
                    githubClient.setHeader("If-Modified-Since", cache.getLastModifiedDateTime(extension));
                }
                HtmlContentsResponse response = githubClient.getContentsResponse(HtmlContentsResponse.class);
                /* API throttling limit reached, could not process further without credentials */
                if (response.getStatus() == 403) {
                    throttled = true;
                    break;
                }
                updateCache(extension, response);
            }

            cache.removeComplementOf(new TreeSet<>(extensions));

            if (!throttled) {
                cache.commit();
            }
        } catch (IOException | ReflectiveOperationException e) {
            return false;
        }
        return true;
    }

    /**
     * This updateCache method updates the underline cache according to API responses.
     *
     * @param extension the name of the extension
     * @param response the API response.
     * @throws IOException if error occurred while extracting the JSON type error.
     */
    private void updateCache(String extension, HtmlContentsResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
            case 200: {
                ContentsBodyReader reader = response.getContentsBodyReader();
                String firstParagraph = reader.getFirstParagraph();
                if (firstParagraph == null) {
                    return;
                }
                cache.add(extension, firstParagraph, response.getHeader("Last-Modified").get(0));
                break;
            }
            case 304:
                break;
            case 404:
                cache.remove(extension);
                break;
            default:
                log.error(String.format("Error occurred while retrieving the extension '%s': %d %s",
                        extension, status, response.getError().toString()));
        }
    }

    /**
     * @return whether API has reached it's throttling limits.
     */
    public boolean isThrottled() {
        return throttled;
    }
}
