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
package org.wso2.siddhi.query.api.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SerializedObject implements java.io.Serializable {
    /**
     * for serialization
     */
    private static final long serialVersionUID = 6635502953928860434L;

    /**
     * The array storing the object.
     */
    private byte[] m_storedObjectArray;

    /**
     * Whether or not the object is compressed.
     */
    private boolean m_isCompressed;

    /**
     * Creates a new serialized object (without compression).
     *
     * @param toStore the object to store
     * @throws Exception if the object couldn't be serialized
     */
    public SerializedObject(Object toStore) throws Exception {

        this(toStore, false);
    }

    /**
     * Creates a new serialized object.
     *
     * @param toStore  the object to store
     * @param compress whether or not to use compression
     * @throws Exception if the object couldn't be serialized
     */
    public SerializedObject(Object toStore, boolean compress) throws Exception {

        ByteArrayOutputStream ostream = new ByteArrayOutputStream();
        OutputStream os = ostream;
        ObjectOutputStream p;
        if (!compress)
            p = new ObjectOutputStream(new BufferedOutputStream(os));
        else
            p = new ObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(os)));
        p.writeObject(toStore);
        p.flush();
        p.close(); // used to be ostream.close() !
        m_storedObjectArray = ostream.toByteArray();

        m_isCompressed = compress;
    }

    /*
     * Checks to see whether this object is equal to another.
     *
     * @param compareTo the object to compare to
     * @return whether or not the objects are equal
     */
    public final boolean equals(Object compareTo) {

        if (compareTo == null) return false;
        if (!compareTo.getClass().equals(this.getClass())) return false;
        byte[] compareArray = ((SerializedObject) compareTo).m_storedObjectArray;
        if (compareArray.length != m_storedObjectArray.length) return false;
        for (int i = 0; i < compareArray.length; i++) {
            if (compareArray[i] != m_storedObjectArray[i]) return false;
        }
        return true;
    }

    /**
     * Returns a hashcode for this object.
     *
     * @return the hashcode
     */
    public int hashCode() {

        return m_storedObjectArray.length;
    }

    /**
     * Returns a serialized object. Uses org.python.util.PythonObjectInputStream
     * for Jython objects (read
     * <a href="http://aspn.activestate.com/ASPN/Mail/Message/Jython-users/1001401">here</a>
     * for more details).
     *
     * @return the restored object
     * @throws Exception if the object couldn't be restored
     */
    public Object getObject() {

        try {
            ByteArrayInputStream istream = new ByteArrayInputStream(m_storedObjectArray);
            ObjectInputStream p;
            Object toReturn = null;
            if (!m_isCompressed)
                p = new ObjectInputStream(new BufferedInputStream(istream));
            else
                p = new ObjectInputStream(new BufferedInputStream(new GZIPInputStream(istream)));
            toReturn = p.readObject();
            istream.close();
            return toReturn;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
