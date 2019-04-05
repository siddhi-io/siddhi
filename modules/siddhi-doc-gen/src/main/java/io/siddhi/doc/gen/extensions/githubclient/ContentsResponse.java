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

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

/**
 * The ContentsResponse class represents an abstract view of the Github Contents API response.
 * If the response is valid it will contain an instance of T type class representing response body.
 * If the response is invalid it will contain an instance of JSONObject class representing the error.
 *
 * @param <T> the type of the valid content body
 */
public abstract class ContentsResponse<T> {

    /**
     * Response body.
     */
    final InputStream stream;

    /**
     * HTTP status code in response.
     */
    private final int status;

    private final Map<String, List<String>> headers;

    /**
     * Reader to read the response body.
     */
    ContentsBodyReader contentsBodyReader;

    ContentsResponse(HttpsURLConnection connection) throws IOException {
        connection.setRequestProperty("Accept", "application/vnd.githubclient.v3." + mediaType());

        status = connection.getResponseCode();
        stream = (status == 200) ? connection.getInputStream() : connection.getErrorStream();

        headers = connection.getHeaderFields();

        contentsBodyReader = null;
    }

    /**
     * @return the media type of the body
     */
    abstract String mediaType();

    /**
     * @return the body of the response
     * @throws IOException if error occurs while parsing the response body as a type T instance
     */
    public abstract T getContent() throws IOException;

    public List<String> getHeader(String name) {
        return headers.get(name);
    }

    /**
     * @return the ContentsBodyReader instance
     */
    public ContentsBodyReader getContentsBodyReader() {
        if (contentsBodyReader == null) {
            throw new IllegalStateException(this.getClass().getCanonicalName()
                    + " does not implement a subclass of "
                    + ContentsBodyReader.class.getCanonicalName());
        }
        return contentsBodyReader;
    }

    /**
     * Because of all API error responses are in JSON format the getError method always returns
     * an instance of {@code org.json.JSONObject}.
     *
     * @return the error message as a JSONObject
     * @throws IOException if error occurs while parsing the response body as a JSONObject
     */
    public JSONObject getError() throws IOException {
        if (status == 200) {
            throw new IllegalStateException("Response does not contain an error.");
        }
        if (stream == null) {
            return new JSONObject();
        }
        return new JSONObject(IOUtils.toString(stream, "UTF-8"));
    }

    /**
     * @return the HTTP status code in the response.
     */
    public int getStatus() {
        return status;
    }
}
