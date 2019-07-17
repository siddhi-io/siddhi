/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.attributes;

/**
 * Instead of using plain objects, or an enum, as keys to user data, instances
 * of {@link UserAttributeKey} carry type information with them.  At run time,
 * makes no difference (type erasure) but makes the developer experience sightly
 * better as avoids unnecessary casts when consuming values.  For example:
 *
 * <pre>{@code
 * UserAttribute<MyData> key = new UserAttribute("myDataKey");
 * SiddhiManager manager = new SiddhiManager();
 * manager.getUserData().put(key, new MyData());
 *
 * ...
 * // Casting and types are inferred since key is defined with <MyData>
 * MyData info = manager.getUserData.get(key);
 * }
 * </pre>
 * <p>
 * Whilst this is not perhaps a common pattern in Java, it works rather well with
 * other JVM languages like Kotlin.
 *
 * @param <T> The type attributed to this key.
 */
@SuppressWarnings("unused")
public class UserAttributeKey<T> {

    private final String name;

    /**
     * Builds a new instance
     *
     * @param name Name associated with this key.  Useful for reporting and debugging.
     */
    public UserAttributeKey(String name) {
        this.name = name;
    }

    /**
     * Name associated with this key.
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "UserAttributeKey: " + this.name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
