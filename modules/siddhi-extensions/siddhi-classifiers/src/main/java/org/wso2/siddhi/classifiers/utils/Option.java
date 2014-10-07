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
package org.wso2.siddhi.classifiers.utils;

public class Option {

    /** What does this option do? */
    private String description;

    /** The synopsis. */
    private String synopsis;

    /** What's the option's name? */
    private String name;

    /** How many arguments does it take? */
    private int numArguments;

    /**
     * Creates new option with the given parameters.
     *
     * @param description the option's description
     * @param name the option's name
     * @param numArguments the number of arguments
     */
    public Option(String description, String name,
                  int numArguments, String synopsis) {

        this.description = description;
        this.name = name;
        this.numArguments = numArguments;
        this.synopsis = synopsis;
    }

    /**
     * Returns the option's description.
     *
     * @return the option's description
     */
    public String description() {

        return description;
    }

    /**
     * Returns the option's name.
     *
     * @return the option's name
     */
    public String name() {

        return name;
    }

    /**
     * Returns the option's number of arguments.
     *
     * @return the option's number of arguments
     */
    public int numArguments() {

        return numArguments;
    }

    /**
     * Returns the option's synopsis.
     *
     * @return the option's synopsis
     */
    public String synopsis() {

        return synopsis;
    }
}
