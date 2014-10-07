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

import org.wso2.siddhi.classifiers.trees.ht.exception.UnassignedClassException;
import org.wso2.siddhi.classifiers.trees.ht.exception.UnassignedDatasetException;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Enumeration;

public abstract class AbstractInstance implements Instance {
    /**
     * The dataset the instance has access to. Null if the instance doesn't have
     * access to any dataset. Only if an instance has access to a dataset, it
     * knows about the actual attribute types.
     */
    protected Instances dataSet;

    /** The instance's attribute values. */
    protected double[] attributeValues;

    /** The instance's weight. */
    protected double weight;

    /** Default max number of digits after the decimal point for numeric values */
    public static int s_numericAfterDecimalPoint = 6;

    /**
     * Returns the attribute with the given index.
     *
     * @param index the attribute's index
     * @return the attribute at the given position
     * @throws UnassignedDatasetException if instance doesn't have access to a
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public Attribute attribute(int index) {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.attribute(index);
    }

    /**
     * Returns the attribute with the given index in the sparse representation.
     *
     * @param indexOfIndex the index of the attribute's index
     * @return the attribute at the given position
     * @throws UnassignedDatasetException if instance doesn't have access to a
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public Attribute attributeSparse(int indexOfIndex) {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.attribute(index(indexOfIndex));
    }

    /**
     * Returns class attribute.
     *
     * @return the class attribute
     * @throws UnassignedDatasetException if the class is not set or the instance
     *           doesn't have access to a dataset
     */
    // @ requires dataSet != null;
    @Override
    public Attribute classAttribute() {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.classAttribute();
    }

    /**
     * Returns the class attribute's index.
     *
     * @return the class index as an integer
     * @throws UnassignedDatasetException if instance doesn't have access to a
     *           dataset
     */
    // @ requires dataSet != null;
    // @ ensures \result == dataSet.classIndex();
    @Override
    public int classIndex() {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.classIndex();
    }

    /**
     * Tests if an instance's class is missing.
     *
     * @return true if the instance's class is missing
     * @throws UnassignedClassException if the class is not set or the instance
     *           doesn't have access to a dataset
     */
    // @ requires classIndex() >= 0;
    @Override
    public boolean classIsMissing() {

        int classIndex = classIndex();
        if (classIndex < 0) {
            throw new UnassignedClassException("Class is not set!");
        }
        return isMissing(classIndex);
    }

    /**
     * Returns an instance's class value in internal format. (ie. as a
     * floating-point number)
     *
     * @return the corresponding value as a double (If the corresponding attribute
     *         is nominal (or a string) then it returns the value's index as a
     *         double).
     * @throws UnassignedClassException if the class is not set or the instance
     *           doesn't have access to a dataset
     */
    // @ requires classIndex() >= 0;
    @Override
    public double classValue() {

        int classIndex = classIndex();
        if (classIndex < 0) {
            throw new UnassignedClassException("Class is not set!");
        }
        return value(classIndex);
    }

    /**
     * Returns the dataset this instance has access to. (ie. obtains information
     * about attribute types from) Null if the instance doesn't have access to a
     * dataset.
     *
     * @return the dataset the instance has accesss to
     */
    // @ ensures \result == dataSet;
    @Override
    public Instances dataset() {

        return dataSet;
    }

    /**
     * Deletes an attribute at the given position (0 to numAttributes() - 1). Only
     * succeeds if the instance does not have access to any dataset because
     * otherwise inconsistencies could be introduced.
     *
     * @param position the attribute's position
     * @throws RuntimeException if the instance has access to a dataset
     */
    // @ requires dataSet != null;
    @Override
    public void deleteAttributeAt(int position) {

        if (dataSet != null) {
            throw new RuntimeException("Instance has access to a dataset!");
        }
        forceDeleteAttributeAt(position);
    }

    /**
     * Returns an enumeration of all the attributes.
     *
     * @return enumeration of all the attributes
     * @throws UnassignedDatasetException if the instance doesn't have access to a
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public Enumeration<Attribute> enumerateAttributes() {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.enumerateAttributes();
    }

    /**
     * Tests if the headers of two instances are equivalent.
     *
     * @param inst another instance
     * @return true if the header of the given instance is equivalent to this
     *         instance's header
     * @throws UnassignedDatasetException if instance doesn't have access to any
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public boolean equalHeaders(Instance inst) {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.equalHeaders(inst.dataset());
    }

    /**
     * Checks if the headers of two instances are equivalent. If not, then returns
     * a message why they differ.
     *
     * @return null if the header of the given instance is equivalent to this
     *         instance's header, otherwise a message with details on why they
     *         differ
     */
    @Override
    public String equalHeadersMsg(Instance inst) {
        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }

        return dataSet.equalHeadersMsg(inst.dataset());
    }

    /**
     * Tests whether an instance has a missing value. Skips the class attribute if
     * set.
     *
     * @return true if instance has a missing value.
     * @throws UnassignedDatasetException if instance doesn't have access to any
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public boolean hasMissingValue() {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        int classIndex = classIndex();
        for (int i = 0; i < numValues(); i++) {
            if (index(i) != classIndex) {
                if (isMissingSparse(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Inserts an attribute at the given position (0 to numAttributes()). Only
     * succeeds if the instance does not have access to any dataset because
     * otherwise inconsistencies could be introduced.
     *
     * @param position the attribute's position
     * @throws RuntimeException if the instance has accesss to a dataset
     * @throws IllegalArgumentException if the position is out of range
     */
    // @ requires dataSet == null;
    // @ requires 0 <= position && position <= numAttributes();
    @Override
    public void insertAttributeAt(int position) {

        if (dataSet != null) {
            throw new RuntimeException("Instance has accesss to a dataset!");
        }
        if ((position < 0) || (position > numAttributes())) {
            throw new IllegalArgumentException("Can't insert attribute: index out "
                    + "of range");
        }
        forceInsertAttributeAt(position);
    }

    /**
     * Tests if a specific value is "missing".
     *
     * @param attIndex the attribute's index
     * @return true if the value is "missing"
     */
    @Override
    public boolean isMissing(int attIndex) {

        if (Utils.isMissingValue(value(attIndex))) {
            return true;
        }
        return false;
    }

    /**
     * Tests if a specific value is "missing", given an index in the sparse
     * representation.
     *
     * @param indexOfIndex the index of the attribute's index
     * @return true if the value is "missing"
     */
    @Override
    public boolean isMissingSparse(int indexOfIndex) {

        if (Utils.isMissingValue(valueSparse(indexOfIndex))) {
            return true;
        }
        return false;
    }

    /**
     * Tests if a specific value is "missing". The given attribute has to belong
     * to a dataset.
     *
     * @param att the attribute
     * @return true if the value is "missing"
     */
    @Override
    public boolean isMissing(Attribute att) {

        return isMissing(att.index());
    }

    /**
     * Returns the number of class labels.
     *
     * @return the number of class labels as an integer if the class attribute is
     *         nominal, 1 otherwise.
     * @throws UnassignedDatasetException if instance doesn't have access to any
     *           dataset
     */
    // @ requires dataSet != null;
    @Override
    public int numClasses() {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return dataSet.numClasses();
    }

    /**
     * Sets the class value of an instance to be "missing". A deep copy of the
     * vector of attribute values is performed before the value is set to be
     * missing.
     *
     * @throws UnassignedClassException if the class is not set
     * @throws UnassignedDatasetException if the instance doesn't have access to a
     *           dataset
     */
    // @ requires classIndex() >= 0;
    @Override
    public void setClassMissing() {

        int classIndex = classIndex();
        if (classIndex < 0) {
            throw new UnassignedClassException("Class is not set!");
        }
        setMissing(classIndex);
    }

    /**
     * Sets the class value of an instance to the given value (internal
     * floating-point format). A deep copy of the vector of attribute values is
     * performed before the value is set.
     *
     * @param value the new attribute value (If the corresponding attribute is
     *          nominal (or a string) then this is the new value's index as a
     *          double).
     * @throws UnassignedClassException if the class is not set
     */
    // @ requires classIndex() >= 0;
    @Override
    public void setClassValue(double value) {

        int classIndex = classIndex();
        if (classIndex < 0) {
            throw new UnassignedClassException("Class is not set!");
        }
        setValue(classIndex, value);
    }

    /**
     * Sets the class value of an instance to the given value. A deep copy of the
     * vector of attribute values is performed before the value is set.
     *
     * @param value the new class value (If the class is a string attribute and
     *          the value can't be found, the value is added to the attribute).
     * @throws UnassignedClassException if the class is not set
     * @throws UnassignedDatasetException if the dataset is not set
     * @throws IllegalArgumentException if the attribute is not nominal or a
     *           string, or the value couldn't be found for a nominal attribute
     */
    // @ requires classIndex() >= 0;
    @Override
    public final void setClassValue(String value) {

        int classIndex = classIndex();
        if (classIndex < 0) {
            throw new UnassignedClassException("Class is not set!");
        }
        setValue(classIndex, value);
    }

    /**
     * Sets the reference to the dataset. Does not check if the instance is
     * compatible with the dataset. Note: the dataset does not know about this
     * instance. If the structure of the dataset's header gets changed, this
     * instance will not be adjusted automatically.
     *
     * @param instances the reference to the dataset
     */
    @Override
    public final void setDataset(Instances instances) {

        dataSet = instances;
    }

    /**
     * Sets a specific value to be "missing". Performs a deep copy of the vector
     * of attribute values before the value is set to be missing.
     *
     * @param attIndex the attribute's index
     */
    @Override
    public final void setMissing(int attIndex) {

        setValue(attIndex, Utils.missingValue());
    }

    /**
     * Sets a specific value to be "missing". Performs a deep copy of the vector
     * of attribute values before the value is set to be missing. The given
     * attribute has to belong to a dataset.
     *
     * @param att the attribute
     */
    @Override
    public final void setMissing(Attribute att) {

        setMissing(att.index());
    }

    /**
     * Sets a value of a nominal or string attribute to the given value. Performs
     * a deep copy of the vector of attribute values before the value is set.
     *
     * @param attIndex the attribute's index
     * @param value the new attribute value (If the attribute is a string
     *          attribute and the value can't be found, the value is added to the
     *          attribute).
     * @throws UnassignedDatasetException if the dataset is not set
     * @throws IllegalArgumentException if the selected attribute is not nominal
     *           or a string, or the supplied value couldn't be found for a
     *           nominal attribute
     */
    // @ requires dataSet != null;
    @Override
    public final void setValue(int attIndex, String value) {

        int valIndex;

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        if (!attribute(attIndex).isNominal() && !attribute(attIndex).isString()) {
            throw new IllegalArgumentException(
                    "Attribute neither nominal nor string!");
        }
        valIndex = attribute(attIndex).indexOfValue(value);
        if (valIndex == -1) {
            if (attribute(attIndex).isNominal()) {
                throw new IllegalArgumentException(
                        "Value not defined for given nominal attribute!");
            } else {
                attribute(attIndex).forceAddValue(value);
                valIndex = attribute(attIndex).indexOfValue(value);
            }
        }
        setValue(attIndex, valIndex);
    }

    /**
     * Sets a specific value in the instance to the given value (internal
     * floating-point format). Performs a deep copy of the vector of attribute
     * values before the value is set, so if you are planning on calling setValue
     * many times it may be faster to create a new instance using toDoubleArray.
     * The given attribute has to belong to a dataset.
     *
     * @param att the attribute
     * @param value the new attribute value (If the corresponding attribute is
     *          nominal (or a string) then this is the new value's index as a
     *          double).
     */
    @Override
    public final void setValue(Attribute att, double value) {

        setValue(att.index(), value);
    }

    /**
     * Sets a value of an nominal or string attribute to the given value. Performs
     * a deep copy of the vector of attribute values before the value is set, so
     * if you are planning on calling setValue many times it may be faster to
     * create a new instance using toDoubleArray. The given attribute has to
     * belong to a dataset.
     *
     * @param att the attribute
     * @param value the new attribute value (If the attribute is a string
     *          attribute and the value can't be found, the value is added to the
     *          attribute).
     * @throws IllegalArgumentException if the the attribute is not nominal or a
     *           string, or the value couldn't be found for a nominal attribute
     */
    @Override
    public final void setValue(Attribute att, String value) {

        if (!att.isNominal() && !att.isString()) {
            throw new IllegalArgumentException(
                    "Attribute neither nominal nor string!");
        }
        int valIndex = att.indexOfValue(value);
        if (valIndex == -1) {
            if (att.isNominal()) {
                throw new IllegalArgumentException(
                        "Value not defined for given nominal attribute!");
            } else {
                att.forceAddValue(value);
                valIndex = att.indexOfValue(value);
            }
        }
        setValue(att.index(), valIndex);
    }

    /**
     * Sets the weight of an instance.
     *
     * @param weight the weight
     */
    @Override
    public final void setWeight(double weight) {

        this.weight = weight;
    }

    /**
     * Returns the relational value of a relational attribute.
     *
     * @param attIndex the attribute's index
     * @return the corresponding relation as an Instances object
     * @throws IllegalArgumentException if the attribute is not a relation-valued
     *           attribute
     * @throws UnassignedDatasetException if the instance doesn't belong to a
     *           dataset.
     */
    // @ requires dataSet != null;
    @Override
    public final Instances relationalValue(int attIndex) {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return relationalValue(dataSet.attribute(attIndex));
    }



    /**
     * Returns the value of a nominal, string, date, or relational attribute for
     * the instance as a string.
     *
     * @param attIndex the attribute's index
     * @return the value as a string
     * @throws IllegalArgumentException if the attribute is not a nominal, string,
     *           date, or relation-valued attribute.
     * @throws UnassignedDatasetException if the instance doesn't belong to a
     *           dataset.
     */
    // @ requires dataSet != null;
    @Override
    public final String stringValue(int attIndex) {

        if (dataSet == null) {
            throw new UnassignedDatasetException(
                    "Instance doesn't have access to a dataset!");
        }
        return stringValue(dataSet.attribute(attIndex));
    }

    /**
     * Returns the value of a nominal, string, date, or relational attribute for
     * the instance as a string.
     *
     * @param att the attribute
     * @return the value as a string
     * @throws IllegalArgumentException if the attribute is not a nominal, string,
     *           date, or relation-valued attribute.
     * @throws UnassignedDatasetException if the instance doesn't belong to a
     *           dataset.
     */
    @Override
    public final String stringValue(Attribute att) {

        int attIndex = att.index();
        if (isMissing(attIndex)) {
            return "?";
        }
        switch (att.getType()) {
            case NOMINAL:
            case STRING:
                return att.value((int) value(attIndex));
            default:
                throw new IllegalArgumentException(
                        "Attribute isn't nominal, string or date!");
        }
    }

    /**
     * Returns the description of one instance with any numeric values printed at
     * the supplied maximum number of decimal places. If the instance doesn't have
     * access to a dataset, it returns the internal floating-point values. Quotes
     * string values that contain whitespace characters.
     *
     * @param afterDecimalPoint the maximum number of digits permitted after the
     *          decimal point for a numeric value
     *
     * @return the instance's description as a string
     */
    @Override
    public final String toStringMaxDecimalDigits(int afterDecimalPoint) {
        StringBuffer text = new StringBuffer(toStringNoWeight(afterDecimalPoint));

        if (weight != 1.0) {
            text.append(",{" + Utils.doubleToString(weight, afterDecimalPoint)
                    + "}");
        }

        return text.toString();
    }

    /**
     * Returns the description of one instance. If the instance doesn't have
     * access to a dataset, it returns the internal floating-point values. Quotes
     * string values that contain whitespace characters.
     *
     * @return the instance's description as a string
     */
    @Override
    public String toString() {

        return toStringMaxDecimalDigits(s_numericAfterDecimalPoint);
    }

    /**
     * Returns the description of one value of the instance as a string. If the
     * instance doesn't have access to a dataset, it returns the internal
     * floating-point value. Quotes string values that contain whitespace
     * characters, or if they are a question mark.
     *
     * @param attIndex the attribute's index
     * @return the value's description as a string
     */
    @Override
    public final String toString(int attIndex) {
        return toString(attIndex, s_numericAfterDecimalPoint);
    }

    /**
     * Returns the description of one value of the instance as a string. If the
     * instance doesn't have access to a dataset, it returns the internal
     * floating-point value. Quotes string values that contain whitespace
     * characters, or if they are a question mark.
     *
     * @param attIndex the attribute's index
     * @param afterDecimalPoint the maximum number of digits permitted after the
     *          decimal point for numeric values
     * @return the value's description as a string
     */
    @Override
    public final String toString(int attIndex, int afterDecimalPoint) {

        StringBuffer text = new StringBuffer();

        if (isMissing(attIndex)) {
            text.append("?");
        } else {
            if (dataSet == null) {
                text.append(Utils.doubleToString(value(attIndex), afterDecimalPoint));
            } else {
                switch (dataSet.attribute(attIndex).getType()) {
                    case NOMINAL:
                    case STRING:
                    case DATE:
                    case RELATIONAL:
                        text.append(Utils.quote(stringValue(attIndex)));
                        break;
                    case NUMERIC:
                        text.append(Utils.doubleToString(value(attIndex), afterDecimalPoint));
                        break;
                    default:
                        throw new IllegalStateException("Unknown attribute type");
                }
            }
        }
        return text.toString();
    }

    /**
     * Returns the description of one value of the instance as a string. If the
     * instance doesn't have access to a dataset it returns the internal
     * floating-point value. Quotes string values that contain whitespace
     * characters, or if they are a question mark. The given attribute has to
     * belong to a dataset.
     *
     * @param att the attribute
     * @return the value's description as a string
     */
    @Override
    public final String toString(Attribute att) {

        return toString(att.index());
    }

    /**
     * Returns the description of one value of the instance as a string. If the
     * instance doesn't have access to a dataset it returns the internal
     * floating-point value. Quotes string values that contain whitespace
     * characters, or if they are a question mark. The given attribute has to
     * belong to a dataset.
     *
     * @param att the attribute
     * @param afterDecimalPoint the maximum number of decimal places to print
     * @return the value's description as a string
     */
    @Override
    public final String toString(Attribute att, int afterDecimalPoint) {

        return toString(att.index(), afterDecimalPoint);
    }

    /**
     * Returns an instance's attribute value in internal format. The given
     * attribute has to belong to a dataset.
     *
     * @param att the attribute
     * @return the specified value as a double (If the corresponding attribute is
     *         nominal (or a string) then it returns the value's index as a
     *         double).
     */
    @Override
    public double value(Attribute att) {

        return value(att.index());
    }

    /**
     * Returns an instance's attribute value in internal format, given an index in
     * the sparse representation.
     *
     * @param indexOfIndex the index of the attribute's index
     * @return the specified value as a double (If the corresponding attribute is
     *         nominal (or a string) then it returns the value's index as a
     *         double).
     */
    @Override
    public double valueSparse(int indexOfIndex) {

        return attributeValues[indexOfIndex];
    }

    /**
     * Returns the instance's weight.
     *
     * @return the instance's weight as a double
     */
    @Override
    public final double weight() {

        return weight;
    }

    /**
     * Deletes an attribute at the given position (0 to numAttributes() - 1).
     *
     * @param position the attribute's position
     */
    protected abstract void forceDeleteAttributeAt(int position);

    /**
     * Inserts an attribute at the given position (0 to numAttributes()) and sets
     * its value to be missing.
     *
     * @param position the attribute's position
     */
    protected abstract void forceInsertAttributeAt(int position);
}
