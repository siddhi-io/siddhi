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
package org.wso2.siddhi.query.api.definition;

import org.wso2.siddhi.query.api.utils.SerializedObject;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Stores information for nominal and string attributes.
 */
public class NominalAttributeInfo implements AttributeInfo {
    private static int STRING_COMPRESS_THRESHOLD = 200;

    /** The attribute's values. */
    protected ArrayList<Object> m_Values;

    /** Mapping of values to indices. */
    protected Hashtable<Object, Integer> m_Hashtable;

    /**
     * Constructs the info based on argument.
     */
    public NominalAttributeInfo(List<String> attributeValues, String attributeName) {

        if (attributeValues == null) {
            m_Values = new ArrayList<Object>();
            m_Hashtable = new Hashtable<Object, Integer>();
        } else {
            m_Values = new ArrayList<Object>(attributeValues.size());
            m_Hashtable = new Hashtable<Object, Integer>(attributeValues.size());
            for (int i = 0; i < attributeValues.size(); i++) {
                Object store = attributeValues.get(i);
                if (((String) store).length() > STRING_COMPRESS_THRESHOLD) {
                    try {
                        store = new SerializedObject(attributeValues.get(i), true);
                    } catch (Exception ex) {
                        System.err.println("Couldn't compress nominal attribute value -"
                                + " storing uncompressed.");
                    }
                }
                if (m_Hashtable.containsKey(store)) {
                    throw new IllegalArgumentException("A nominal attribute ("
                            + attributeName + ") cannot" + " have duplicate labels (" + store
                            + ").");
                }
                m_Values.add(store);
                m_Hashtable.put(store, new Integer(i));
            }
        }
    }

    public ArrayList<Object> getM_Values() {
        return m_Values;
    }

    public void setM_Values(ArrayList<Object> m_Values) {
        this.m_Values = m_Values;
    }

    public Hashtable<Object, Integer> getM_Hashtable() {
        return m_Hashtable;
    }

    public void setM_Hashtable(Hashtable<Object, Integer> m_Hashtable) {
        this.m_Hashtable = m_Hashtable;
    }

    public int getNumValues(){
        return m_Values.size();
    }
}
