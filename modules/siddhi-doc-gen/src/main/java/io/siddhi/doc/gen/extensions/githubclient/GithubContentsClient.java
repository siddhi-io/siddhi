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
package io.siddhi.doc.gen.extensions.githubclient;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

/**
 * The GithubContentsClient class offers the ability to make requests to fetch certain
 * contents from Github using Github Contents REST API.
 */
public class GithubContentsClient {

    /**
     * The domain of the API.
     */
    private static final String API_DOMAIN = "api.github.com";

    /**
     * The HTTPS connection to the API.
     */
    private final HttpsURLConnection connection;

    /**
     * The GithubContentsClient builder.
     */
    public static class Builder {
        private final String owner;
        private final String repos;
        private final StringBuilder queryParamsBuilder;

        private boolean isReadme = false;
        private String path = "/";

        public Builder(String owner, String repos) {
            this.owner = owner;
            this.repos = repos;
            queryParamsBuilder = new StringBuilder();
        }

        public Builder isReadme(boolean isReadme) {
            this.isReadme = isReadme;
            return this;
        }

        public Builder path(String path) {
            if (!path.isEmpty() && path.charAt(0) != '/') {
                path = "/" + path;
            }
            this.path = path;
            return this;
        }

        public Builder queryParam(String key, String val) {
            if (queryParamsBuilder.length() != 0) {
                queryParamsBuilder.append("&");
            }
            queryParamsBuilder.append(key).append("=").append(val);
            return this;
        }

        public GithubContentsClient build() throws IOException {
            return new GithubContentsClient(this);
        }
    }

    private GithubContentsClient(Builder builder) throws IOException {
        StringBuilder urlBuilder = new StringBuilder()
                .append("https://")
                .append(API_DOMAIN)
                .append("/repos")
                .append("/").append(builder.owner)
                .append("/").append(builder.repos);
        if (builder.isReadme) {
            urlBuilder.append("/readme");
        } else {
            urlBuilder.append("/contents").append(builder.path);
        }
        String queryParams = builder.queryParamsBuilder.toString();
        if (!queryParams.isEmpty()) {
            urlBuilder.append("?").append(queryParams);
        }
        URL url = new URL(urlBuilder.toString());
        connection = (HttpsURLConnection) url.openConnection();
    }

    /**
     * Sets a header in HTTPS request.
     *
     * @param key the name of the header
     * @param val the value of the header
     */
    public void setHeader(String key, String val) {
        connection.setRequestProperty(key, val);
    }

    /**
     * The getContentsResponse method returns an instance of class T which extends the
     * {@code ContentsResponse} class.
     *
     * @param tClass the Class instance of the type T
     * @param <T> the class which holds API response
     * @return a instance of ContentsResponse
     * @throws ReflectiveOperationException if tClass does not exist or
     * if constructor fails to create an instance of T
     */
    public <T extends ContentsResponse> T getContentsResponse(Class<T> tClass) throws ReflectiveOperationException {
        Constructor<T> constructor = tClass.getDeclaredConstructor(HttpsURLConnection.class);
        constructor.setAccessible(true);
        return constructor.newInstance(connection);
    }
}
