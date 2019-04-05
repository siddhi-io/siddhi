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

/**
 * The ContentsBodyReader class offers ability to read from a C type response body
 * from a Github Contents API response.
 *
 * @param <C> the response body type
 */
public abstract class ContentsBodyReader<C> {

    /**
     * The C type content.
     */
    protected final C content;

    ContentsBodyReader(C content) {
        this.content = content;
    }

    /**
     * Extract the first paragraph from the whole content body
     * and return it as a String.
     *
     * @return the extracted paragraph
     */
    public abstract String getFirstParagraph();
}
