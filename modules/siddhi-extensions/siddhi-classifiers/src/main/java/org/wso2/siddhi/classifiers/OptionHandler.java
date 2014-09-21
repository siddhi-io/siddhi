/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers;

import org.wso2.siddhi.classifiers.utils.Option;

import java.util.Enumeration;

public interface OptionHandler {
    /**
     * Returns an enumeration of all the available options..
     *
     * @return an enumeration of all available options.
     */
    Enumeration<Option> listOptions();

    /**
     * Sets the OptionHandler's options using the given list. All options
     * will be set (or reset) during this call (i.e. incremental setting
     * of options is not possible).
     *
     * @param options the list of options as an array of strings
     * @exception Exception if an option is not supported
     */
    //@ requires options != null;
    //@ requires \nonnullelements(options);
    void setOptions(String[] options) throws Exception;

    /**
     * Gets the current option settings for the OptionHandler.
     *
     * @return the list of current option settings as an array of strings
     */
    //@ ensures \result != null;
    //@ ensures \nonnullelements(\result);
  /*@pure@*/ String[] getOptions();
}
