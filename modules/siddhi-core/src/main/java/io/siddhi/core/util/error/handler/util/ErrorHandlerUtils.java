/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.error.handler.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Contains utility methods for the error handler.
 */
public class ErrorHandlerUtils {

    private ErrorHandlerUtils() {}

    public static byte[] getAsBytes(Object event) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(event);
        return baos.toByteArray();
    }

    public static Object getAsObject(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream baip = new ByteArrayInputStream(byteArray);
        ObjectInputStream ois = new ObjectInputStream(baip);
        return ois.readObject();
    }

    public static byte[] getThrowableStackTraceAsBytes(Throwable throwable) throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return getAsBytes(sw.toString());
    }

    public static String getOriginalPayloadString(Object originalPayloadAsObject) {
        if (originalPayloadAsObject != null) {
            return originalPayloadAsObject.toString();
        }
        return null;
    }
}
