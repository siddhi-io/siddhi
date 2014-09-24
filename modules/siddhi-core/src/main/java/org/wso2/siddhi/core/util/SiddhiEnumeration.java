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
package org.wso2.siddhi.core.util;

import java.util.Enumeration;
import java.util.List;

public class SiddhiEnumeration <E> implements Enumeration<E> {

    /** The counter. */
    private int m_Counter;
    // These JML commands say how m_Counter implements Enumeration
    // @ in moreElements;
    // @ private represents moreElements = m_Counter < m_Vector.size();
    // @ private invariant 0 <= m_Counter && m_Counter <= m_Vector.size();

    /** The vector. */
    private final List<E> m_Vector;

    /** Special element. Skipped during enumeration. */
    private final int m_SpecialElement;

    // @ private invariant -1 <= m_SpecialElement;
    // @ private invariant m_SpecialElement < m_Vector.size();
    // @ private invariant m_SpecialElement>=0 ==> m_Counter!=m_SpecialElement;

    /**
     * Constructs an enumeration.
     *
     * @param vector the vector which is to be enumerated
     */
    public SiddhiEnumeration(List<E> vector) {

        m_Counter = 0;
        m_Vector = vector;
        m_SpecialElement = -1;
    }

    /**
     * Constructs an enumeration with a special element. The special element is
     * skipped during the enumeration.
     *
     * @param vector the vector which is to be enumerated
     * @param special the index of the special element
     */
    // @ requires 0 <= special && special < vector.size();
    public SiddhiEnumeration(List<E> vector, int special) {

        m_Vector = vector;
        m_SpecialElement = special;
        if (special == 0) {
            m_Counter = 1;
        } else {
            m_Counter = 0;
        }
    }

    /**
     * Tests if there are any more elements to enumerate.
     *
     * @return true if there are some elements left
     */
    @Override
    public final boolean hasMoreElements() {

        if (m_Counter < m_Vector.size()) {
            return true;
        }
        return false;
    }

    /**
     * Returns the next element.
     *
     * @return the next element to be enumerated
     */
    // @ also requires hasMoreElements();
    @Override
    public final E nextElement() {

        E result = m_Vector.get(m_Counter);

        m_Counter++;
        if (m_Counter == m_SpecialElement) {
            m_Counter++;
        }
        return result;
    }
}
