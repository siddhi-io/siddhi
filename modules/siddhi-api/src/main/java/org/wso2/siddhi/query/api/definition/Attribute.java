/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.query.api.definition;

import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.utils.CommonUtils;
import org.wso2.siddhi.query.api.utils.SerializedObject;

import java.util.ArrayList;
import java.util.List;

public class Attribute {
    private String name;

    private Type type;

    /**
     * The attribute's weight.
     */
    protected double m_Weight = 1.0;

    /**
     * Strings longer than this will be stored compressed.
     */
    protected static final int STRING_COMPRESS_THRESHOLD = 200;

    private AttributeInfo attributeInfo;

    /**
     * The attribute's index.
     */
    private int index = -1;

    public final static String DUMMY_STRING_VAL = "*SIDDHI*DUMMY*STRING*FOR*STRING*ATTRIBUTES*";


    public Attribute(String name, List<String> attrValues, int index) {
        this.name = name;
        this.index = index;
        attributeInfo = new NominalAttributeInfo(attrValues, name);
        if (attrValues == null) {
            this.type = Type.STRING;

            // Make sure there is at least one value so that string attribute
            // values are always represented when output as part of a sparse instance.
            addStringValue(DUMMY_STRING_VAL);
        } else {
            type = Type.NOMINAL;
        }
    }
    public Attribute(String name, List<String> attrValues) {
        this.name = name;
        attributeInfo = new NominalAttributeInfo(attrValues, name);
        if (attrValues == null) {
            this.type = Type.STRING;

            // Make sure there is at least one value so that string attribute
            // values are always represented when output as part of a sparse instance.
            addStringValue(DUMMY_STRING_VAL);
        } else {
            type = Type.NOMINAL;
        }
    }

    public Attribute(String name, Expression[] expressions) {
        this.name = name;
        ArrayList<String> attributeValues = new ArrayList<String>();
        for(Expression e:expressions){
            if(e instanceof Variable){
                attributeValues.add(((Variable)e).getAttributeName());
            }else {
                throw new RuntimeException("Bad Arguments !");
            }
        }
        attributeInfo = new NominalAttributeInfo(attributeValues, name);
        if (attributeValues == null) {
            this.type = Type.STRING;

            // Make sure there is at least one value so that string attribute
            // values are always represented when output as part of a sparse instance.
            addStringValue(DUMMY_STRING_VAL);
        } else {
            type = Type.NOMINAL;
        }
    }
    public Attribute(String name) {
        this.name = name;
        attributeInfo = new NominalAttributeInfo(null, name);
        this.type = Type.STRING;
        addStringValue(DUMMY_STRING_VAL);
    }

    public boolean isRelationValued() {
        return (type==Type.RELATIONAL);
    }

    public enum Type {
        STRING, INT, LONG, FLOAT, DOUBLE, BOOL, TYPE, NOMINAL, DATE, RELATIONAL, NUMERIC
    }

    public Attribute(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Attribute attribute = (Attribute) o;

        if (name != null ? !name.equals(attribute.name) : attribute.name != null) return false;
        if (type != attribute.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    public double getM_Weight() {
        return m_Weight;
    }

    public void setM_Weight(double m_Weight) {
        this.m_Weight = m_Weight;
    }

    public static int getStringCompressThreshold() {
        return STRING_COMPRESS_THRESHOLD;
    }

    public AttributeInfo getAttributeInfo() {
        return attributeInfo;
    }

    public void setAttributeInfo(AttributeInfo attributeInfo) {
        this.attributeInfo = attributeInfo;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int addStringValue(String value) {

        if (getType() != Type.STRING) {
            return -1;
        }
        Object store = value;

        if (value.length() > STRING_COMPRESS_THRESHOLD) {
            try {
                store = new SerializedObject(store, true);
            } catch (Exception ex) {
                System.err.println("Couldn't compress string attribute value -"
                        + " storing uncompressed.");
            }
        }
        Integer index = ((NominalAttributeInfo) attributeInfo).m_Hashtable.get(store);
        if (index != null) {
            return index.intValue();
        } else {
            int intIndex = ((NominalAttributeInfo) attributeInfo).m_Values.size();
            ((NominalAttributeInfo) attributeInfo).m_Values.add(store);
            ((NominalAttributeInfo) attributeInfo).m_Hashtable.put(store, new Integer(intIndex));
            return intIndex;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isNominal() {
        return Type.NOMINAL.equals(type);
    }

    public boolean isString() {
        return Type.STRING.equals(type);
    }

    public boolean isNumeric() {
        return Type.NUMERIC.equals(type);
    }

    public/* @ pure non_null @ */Object copy() {

        return copy(name);
    }
    public final Attribute copy(String newName) {

        Attribute copy = new Attribute(newName,new ArrayList<String>());

        copy.index = index;
        copy.type = type;
        copy.attributeInfo = attributeInfo;
        return copy;
    }

    public final/* @ pure @ */int numValues() {

        if (!isNominal() && !isString()) {
            return 0;
        } else {
            return ((NominalAttributeInfo)attributeInfo).m_Values.size();
        }
    }


    public final void setValue(int index, String string) {
        switch (type) {
            case NOMINAL:
            case STRING:
                ((NominalAttributeInfo) attributeInfo).m_Values =
                        CommonUtils.cast(((NominalAttributeInfo) attributeInfo).m_Values.clone());
                ((NominalAttributeInfo) attributeInfo).m_Hashtable =
                        CommonUtils.cast(((NominalAttributeInfo) attributeInfo).m_Hashtable.clone());
                Object store=string;
                if (string.length() > STRING_COMPRESS_THRESHOLD) {
                    try {
                        store = new SerializedObject(string, true);
                    } catch (Exception ex) {
                        System.err.println("Couldn't compress string attribute value -"
                                + " storing uncompressed.");
                    }
                }
                ((NominalAttributeInfo) attributeInfo).m_Hashtable.
                        remove(((NominalAttributeInfo) attributeInfo).m_Values.get(index));
                ((NominalAttributeInfo) attributeInfo).m_Values.set(index, store);
                ((NominalAttributeInfo) attributeInfo).m_Hashtable.put(store, new Integer(index));
                break;
            default:
                throw new IllegalArgumentException("Can only set values for nominal"
                        + " or string attributes!");
        }
    }

    /**
     * Returns the index of a given attribute value. (The index of the first
     * occurence of this value.)
     *
     * @param value the value for which the index is to be returned
     * @return the index of the given attribute value if attribute is nominal or a
     *         string, -1 if it is not or the value can't be found
     */
    public final int indexOfValue(String value) {

        if (!isNominal() && !isString()) {
            return -1;
        }
        Object store = value;
        if (value.length() > STRING_COMPRESS_THRESHOLD) {
            try {
                store = new SerializedObject(value,true);
            } catch (Exception ex) {
                System.err.println("Couldn't compress string attribute value -"
                        + " searching uncompressed.");
            }
        }
        Integer val = ((NominalAttributeInfo)attributeInfo).m_Hashtable.get(store);
        if (val == null) {
            return -1;
        } else {
            return val.intValue();
        }
    }

    /**
     * Returns the index of this attribute.
     *
     * @return the index of this attribute
     */
    // @ ensures \result == m_Index;
    public final int index() {

        return index;
    }

    /**
     * Returns a value of a nominal or string attribute. Returns an empty string
     * if the attribute is neither a string nor a nominal attribute.
     *
     * @param valIndex the value's index
     * @return the attribute's value as a string
     */
    public final/* @ non_null pure @ */String value(int valIndex) {

        if (!isNominal() && !isString()) {
            return "";
        } else {
            Object val = ((NominalAttributeInfo)attributeInfo).m_Values.get(valIndex);

            // If we're storing strings compressed, uncompress it.
            if (val instanceof SerializedObject) {
                val = ((SerializedObject) val).getObject();
            }
            return (String) val;
        }
    }
    /**
     * Adds an attribute value.
     *
     * @param value the attribute value
     */
    // @ requires value != null;
    // @ ensures m_Values.size() == \old(m_Values.size()) + 1;
    public final void forceAddValue(String value) {

        Object store = value;
        if (value.length() > STRING_COMPRESS_THRESHOLD) {
            try {
                store = new SerializedObject(value, true);
            } catch (Exception ex) {
                System.err.println("Couldn't compress string attribute value -"
                        + " storing uncompressed.");
            }
        }
        ((NominalAttributeInfo)attributeInfo).m_Values.add(store);
        ((NominalAttributeInfo)attributeInfo).m_Hashtable.
                put(store, new Integer(((NominalAttributeInfo)attributeInfo).m_Values.size() - 1));
    }
}
