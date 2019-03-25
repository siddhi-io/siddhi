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
package io.siddhi.doc.gen.core.githubclient;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import javax.net.ssl.HttpsURLConnection;

public class HtmlContentResponse extends ContentResponse<Document> {

    HtmlContentResponse(HttpsURLConnection connection) throws IOException {
        super(connection);
        super.contentReader = new ContentReader<Document, HtmlContentResponse>(this) {

            @Override
            String getFirstParagraph(Document content) {
                Elements pTags = content.getElementsByTag("p");
                if (pTags != null) {
                    Element firstPTag = pTags.first();
                    if (firstPTag != null) {
                        return firstPTag.text();
                    }
                }
                return null;
            }
        };
    }

    @Override
    String mediaType() {
        return "html";
    }

    @Override
    public Document getContent() throws IOException {
        return Jsoup.parse(super.stream, null, "");
    }
}
