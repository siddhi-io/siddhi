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
 * Container for arbitrary instances indexed by {@link UserAttributeKey} instances
 */
public interface UserAttributes {

    /**
     * Returns the value associated with key.  If the value is not found or if
     * the value is null, it will throw {@link IllegalStateException}
     *
     * @param key Key of the value to retrieve
     * @param <T> The type of the value (as defined by the key)
     * @return A non null value associated with the given key
     */
    default <T> T get(UserAttributeKey<T> key) throws IllegalStateException {
        T value = getOrNull(key);
        if (value == null) {
            throw new IllegalStateException("No Instance for key " + key);
        }

        return value;
    }

    /**
     * Same as {@link UserAttributes#get}, but allows nulls
     *
     * @param key Key of the value to retrieve
     * @param <T> The type of the value (as defined by the key)
     * @return A nullable value associated with the given key
     */
    <T> T getOrNull(UserAttributeKey<T> key);

    /**
     * Returns <tt>true</tt> if this instance contains a mapping for the specified key.
     *
     * @param key
     * @return When this instance contains the specified key, it returns <tt>true</tt>; false otherwise.
     */
    boolean contains(UserAttributeKey<?> key);

    /**
     * Updates this instance with a new value for the specified key.
     *
     * @param key   Key of the value to update
     * @param value An instance of T
     * @param <T>   The type of the value (as defined by the key)
     */
    <T> void put(UserAttributeKey<T> key, T value);

    /**
     * Removes an entry from this instance.
     *
     * @param key Key of the value to update
     * @param <T> The type of the value (as defined by the key)
     * @return The existing value, if there was one.
     */
    <T> T remove(UserAttributeKey<T> key);
}


