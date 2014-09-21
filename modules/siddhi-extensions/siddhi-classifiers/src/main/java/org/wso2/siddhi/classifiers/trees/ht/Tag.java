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
package org.wso2.siddhi.classifiers.trees.ht;

public class Tag {

    /** for serialization. */
    private static final long serialVersionUID = 3326379903447135320L;

    /** The ID */
    protected int m_ID;

    /** The unique string for this tag, doesn't have to be numeric */
    protected String m_IDStr;

    /** The descriptive text */
    protected String m_Readable;

    /**
     * Creates a new default Tag
     *
     */
    public Tag() {
        this(0, "A new tag", "A new tag", true);
    }

    /**
     * Creates a new <code>Tag</code> instance.
     *
     * @param ident the ID for the new Tag.
     * @param readable the description for the new Tag.
     */
    public Tag(int ident, String readable) {
        this(ident, "", readable);
    }

    /**
     * Creates a new <code>Tag</code> instance.
     *
     * @param ident the ID for the new Tag.
     * @param identStr the ID string for the new Tag (case-insensitive).
     * @param readable the description for the new Tag.
     */
    public Tag(int ident, String identStr, String readable) {
        this(ident, identStr, readable, true);
    }

    public Tag(int ident, String identStr, String readable, boolean upperCase) {
        m_ID = ident;
        if (identStr.length() == 0) {
            m_IDStr = "" + ident;
        } else {
            m_IDStr = identStr;
            if (upperCase) {
                m_IDStr = identStr.toUpperCase();
            }
        }
        m_Readable = readable;
    }

    /**
     * Gets the numeric ID of the Tag.
     *
     * @return the ID of the Tag.
     */
    public int getID() {
        return m_ID;
    }

    /**
     * Sets the numeric ID of the Tag.
     *
     * @param id the ID of the Tag.
     */
    public void setID(int id) {
        m_ID = id;
    }

    /**
     * Gets the string ID of the Tag.
     *
     * @return the string ID of the Tag.
     */
    public String getIDStr() {
        return m_IDStr;
    }

    /**
     * Sets the string ID of the Tag.
     *
     * @param str the string ID of the Tag.
     */
    public void setIDStr(String str) {
        m_IDStr = str;
    }

    /**
     * Gets the string description of the Tag.
     *
     * @return the description of the Tag.
     */
    public String getReadable() {
        return m_Readable;
    }

    /**
     * Sets the string description of the Tag.
     *
     * @param r the description of the Tag.
     */
    public void setReadable(String r) {
        m_Readable = r;
    }

    /**
     * returns the IDStr
     *
     * @return the IDStr
     */
    public String toString() {
        return m_IDStr;
    }

    /**
     * returns a list that can be used in the listOption methods to list all
     * the available ID strings, e.g.: &lt;0|1|2&gt; or &lt;what|ever&gt;
     *
     * @param tags the tags to create the list for
     * @return a list of all ID strings
     */
    public static String toOptionList(Tag[] tags) {
        String	result;
        int		i;

        result = "<";
        for (i = 0; i < tags.length; i++) {
            if (i > 0)
                result += "|";
            result += tags[i];
        }
        result += ">";

        return result;
    }

    /**
     * returns a string that can be used in the listOption methods to list all
     * the available options, i.e., "\t\tID = Text\n" for each option
     *
     * @param tags the tags to create the string for
     * @return a string explaining the tags
     */
    public static String toOptionSynopsis(Tag[] tags) {
        String	result;
        int		i;

        result = "";
        for (i = 0; i < tags.length; i++) {
            result += "\t\t" + tags[i].getIDStr() + " = " + tags[i].getReadable() + "\n";
        }

        return result;
    }
}
