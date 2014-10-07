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

import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Enumeration;

public class DenseInstance extends AbstractInstance {
    /**
     * Constructor that copies the attribute values and the weight from the given
     * instance. It does NOT perform a deep copy of the attribute values if the
     * instance provided is also of type DenseInstance (it simply copies the
     * reference to the array of values), otherwise it does. Reference to the
     * dataset is set to null. (ie. the instance doesn't have access to
     * information about the attribute types)
     *
     * @param instance the instance from which the attribute values and the weight
     *          are to be copied
     */
    // @ ensures dataSet == null;
    public DenseInstance(Instance instance) {

        if (instance instanceof DenseInstance) {
            attributeValues = ((DenseInstance) instance).attributeValues;
        } else {
            attributeValues = instance.toDoubleArray();
        }
        weight = instance.weight();
        dataSet = null;
    }

    /**
     * Constructor that inititalizes instance variable with given values.
     * Reference to the dataset is set to null. (ie. the instance doesn't have
     * access to information about the attribute types)
     *
     * @param weight the instance's weight
     * @param attValues a vector of attribute values
     */
    // @ ensures dataSet == null;
    public DenseInstance(double weight, double[] attValues) {

        attributeValues = attValues;
        this.weight = weight;
        dataSet = null;
    }

    /**
     * Constructor of an instance that sets weight to one, all values to be
     * missing, and the reference to the dataset to null. (ie. the instance
     * doesn't have access to information about the attribute types)
     *
     * @param numAttributes the size of the instance
     */
    // @ requires numAttributes > 0; // Or maybe == 0 is okay too?
    // @ ensures dataSet == null;
    public DenseInstance(int numAttributes) {

        attributeValues = new double[numAttributes];
        for (int i = 0; i < attributeValues.length; i++) {
            attributeValues[i] = Utils.missingValue();
        }
        weight = 1;
        dataSet = null;
    }

    /**
     * Produces a shallow copy of this instance. The copy has access to the same
     * dataset. (if you want to make a copy that doesn't have access to the
     * dataset, use <code>new DenseInstance(instance)</code>
     *
     * @return the shallow copy
     */
    // @ also ensures \result != null;
    // @ also ensures \result instanceof DenseInstance;
    // @ also ensures ((DenseInstance)\result).dataSet == dataSet;
    @Override
    public Object copy() {

        DenseInstance result = new DenseInstance(this);
        result.dataSet = dataSet;
        return result;
    }

    /**
     * Returns the index of the attribute stored at the given position. Just
     * returns the given value.
     *
     * @param position the position
     * @return the index of the attribute stored at the given position
     */
    @Override
    public int index(int position) {

        return position;
    }

    /**
     * Merges this instance with the given instance and returns the result.
     * Dataset is set to null. The returned instance is of the same type as this
     * instance.
     *
     * @param inst the instance to be merged with this one
     * @return the merged instances
     */
    @Override
    public Instance mergeInstance(Instance inst) {

        int m = 0;
        double[] newVals = new double[numAttributes() + inst.numAttributes()];
        for (int j = 0; j < numAttributes(); j++, m++) {
            newVals[m] = value(j);
        }
        for (int j = 0; j < inst.numAttributes(); j++, m++) {
            newVals[m] = inst.value(j);
        }
        return new DenseInstance(1.0, newVals);
    }

    /**
     * Returns the number of attributes.
     *
     * @return the number of attributes as an integer
     */
    // @ ensures \result == attributeValues.length;
    @Override
    public int numAttributes() {

        return attributeValues.length;
    }

    /**
     * Returns the number of values present. Always the same as numAttributes().
     *
     * @return the number of values
     */
    // @ ensures \result == attributeValues.length;
    @Override
    public int numValues() {

        return attributeValues.length;
    }

    /**
     * Replaces all missing values in the instance with the values contained in
     * the given array. A deep copy of the vector of attribute values is performed
     * before the values are replaced.
     *
     * @param array containing the means and modes
     * @throws IllegalArgumentException if numbers of attributes are unequal
     */
    @Override
    public void replaceMissingValues(double[] array) {

        if ((array == null) || (array.length != attributeValues.length)) {
            throw new IllegalArgumentException("Unequal number of attributes!");
        }
        freshAttributeVector();
        for (int i = 0; i < attributeValues.length; i++) {
            if (isMissing(i)) {
                attributeValues[i] = array[i];
            }
        }
    }

    /**
     * Sets a specific value in the instance to the given value (internal
     * floating-point format). Performs a deep copy of the vector of attribute
     * values before the value is set.
     *
     * @param attIndex the attribute's index
     * @param value the new attribute value (If the corresponding attribute is
     *          nominal (or a string) then this is the new value's index as a
     *          double).
     */
    @Override
    public void setValue(int attIndex, double value) {

        freshAttributeVector();
        attributeValues[attIndex] = value;
    }

    /**
     * Sets a specific value in the instance to the given value (internal
     * floating-point format). Performs a deep copy of the vector of attribute
     * values before the value is set. Does exactly the same thing as setValue().
     *
     * @param indexOfIndex the index of the attribute's index
     * @param value the new attribute value (If the corresponding attribute is
     *          nominal (or a string) then this is the new value's index as a
     *          double).
     */
    @Override
    public void setValueSparse(int indexOfIndex, double value) {

        freshAttributeVector();
        attributeValues[indexOfIndex] = value;
    }

    /**
     * Returns the values of each attribute as an array of doubles.
     *
     * @return an array containing all the instance attribute values
     */
    @Override
    public double[] toDoubleArray() {

        double[] newValues = new double[attributeValues.length];
        System.arraycopy(attributeValues, 0, newValues, 0, attributeValues.length);
        return newValues;
    }

    /**
     * Returns the description of one instance (without weight appended). If the
     * instance doesn't have access to a dataset, it returns the internal
     * floating-point values. Quotes string values that contain whitespace
     * characters.
     *
     * @return the instance's description as a string
     */
    @Override
    public String toStringNoWeight() {
        return toStringNoWeight(AbstractInstance.s_numericAfterDecimalPoint);
    }

    /**
     * Returns the description of one instance (without weight appended). If the
     * instance doesn't have access to a dataset, it returns the internal
     * floating-point values. Quotes string values that contain whitespace
     * characters.
     *
     *
     * @param afterDecimalPoint maximum number of digits after the decimal point
     *          for numeric values
     *
     * @return the instance's description as a string
     */
    @Override
    public String toStringNoWeight(int afterDecimalPoint) {
        StringBuffer text = new StringBuffer();

        for (int i = 0; i < attributeValues.length; i++) {
            if (i > 0) {
                text.append(",");
            }
            text.append(toString(i, afterDecimalPoint));
        }

        return text.toString();
    }

    /**
     * Returns an instance's attribute value in internal format.
     *
     * @param attIndex the attribute's index
     * @return the specified value as a double (If the corresponding attribute is
     *         nominal (or a string) then it returns the value's index as a
     *         double).
     */
    public double value(int attIndex) {

        return attributeValues[attIndex];
    }

    /**
     * Deletes an attribute at the given position (0 to numAttributes() - 1).
     *
     * @param position the attribute's position
     */
    @Override
    protected void forceDeleteAttributeAt(int position) {

        double[] newValues = new double[attributeValues.length - 1];

        System.arraycopy(attributeValues, 0, newValues, 0, position);
        if (position < attributeValues.length - 1) {
            System.arraycopy(attributeValues, position + 1, newValues, position,
                    attributeValues.length - (position + 1));
        }
        attributeValues = newValues;
    }

    /**
     * Inserts an attribute at the given position (0 to numAttributes()) and sets
     * its value to be missing.
     *
     * @param position the attribute's position
     */
    @Override
    protected void forceInsertAttributeAt(int position) {

        double[] newValues = new double[attributeValues.length + 1];

        System.arraycopy(attributeValues, 0, newValues, 0, position);
        newValues[position] = Utils.missingValue();
        System.arraycopy(attributeValues, position, newValues, position + 1,
                attributeValues.length - position);
        attributeValues = newValues;
    }

    /**
     * Clones the attribute vector of the instance and overwrites it with the
     * clone.
     */
    private void freshAttributeVector() {

        attributeValues = toDoubleArray();
    }

    /**
     * Main method for testing this class.
     *
     * @param options the commandline options - ignored
     */
    // @ requires options != null;
    public static void main(String[] options) {

        try {

            // Create numeric attributes "length" and "weight"
            Attribute length = new Attribute("length");
            Attribute weight = new Attribute("weight");

            // Create vector to hold nominal values "first", "second", "third"
            ArrayList<String> my_nominal_values = new ArrayList<String>(3);
            my_nominal_values.add("first");
            my_nominal_values.add("second");
            my_nominal_values.add("third");

            // Create nominal attribute "position"
            Attribute position = new Attribute("position", my_nominal_values);

            // Create vector of the above attributes
            ArrayList<Attribute> attributes = new ArrayList<Attribute>(3);
            attributes.add(length);
            attributes.add(weight);
            attributes.add(position);

            // Create the empty dataset "race" with above attributes
            Instances race = new Instances("race", attributes, 0);

            // Make position the class attribute
            race.setClassIndex(position.index());

            // Create empty instance with three attribute values
            Instance inst = new DenseInstance(3);

            // Set instance's values for the attributes "length", "weight", and
            // "position"
            inst.setValue(length, 5.3);
            inst.setValue(weight, 300);
            inst.setValue(position, "first");

            // Set instance's dataset to be the dataset "race"
            inst.setDataset(race);

            // Print the instance
            System.out.println("The instance: " + inst);

            // Print the first attribute
            System.out.println("First attribute: " + inst.attribute(0));

            // Print the class attribute
            System.out.println("Class attribute: " + inst.classAttribute());

            // Print the class index
            System.out.println("Class index: " + inst.classIndex());

            // Say if class is missing
            System.out.println("Class is missing: " + inst.classIsMissing());

            // Print the instance's class value in internal format
            System.out.println("Class value (internal format): " + inst.classValue());

            // Print a shallow copy of this instance
            Instance copy = (Instance) inst.copy();
            System.out.println("Shallow copy: " + copy);

            // Set dataset for shallow copy
            copy.setDataset(inst.dataset());
            System.out.println("Shallow copy with dataset set: " + copy);

            // Unset dataset for copy, delete first attribute, and insert it again
            copy.setDataset(null);
            copy.deleteAttributeAt(0);
            copy.insertAttributeAt(0);
            copy.setDataset(inst.dataset());
            System.out.println("Copy with first attribute deleted and inserted: "
                    + copy);

            // Enumerate attributes (leaving out the class attribute)
            System.out.println("Enumerating attributes (leaving out class):");
            Enumeration<Attribute> enu = inst.enumerateAttributes();
            while (enu.hasMoreElements()) {
                Attribute att = enu.nextElement();
                System.out.println(att);
            }

            // Headers are equivalent?
            System.out.println("Header of original and copy equivalent: "
                    + inst.equalHeaders(copy));

            // Test for missing values
            System.out.println("Length of copy missing: " + copy.isMissing(length));
            System.out.println("Weight of copy missing: "
                    + copy.isMissing(weight.index()));
            System.out.println("Length of copy missing: "
                    + Utils.isMissingValue(copy.value(length)));

            // Prints number of attributes and classes
            System.out.println("Number of attributes: " + copy.numAttributes());
            System.out.println("Number of classes: " + copy.numClasses());

            // Replace missing values
            double[] meansAndModes = { 2, 3, 0 };
            copy.replaceMissingValues(meansAndModes);
            System.out.println("Copy with missing value replaced: " + copy);

            // Setting and getting values and weights
            copy.setClassMissing();
            System.out.println("Copy with missing class: " + copy);
            copy.setClassValue(0);
            System.out.println("Copy with class value set to first value: " + copy);
            copy.setClassValue("third");
            System.out.println("Copy with class value set to \"third\": " + copy);
            copy.setMissing(1);
            System.out.println("Copy with second attribute set to be missing: "
                    + copy);
            copy.setMissing(length);
            System.out.println("Copy with length set to be missing: " + copy);
            copy.setValue(0, 0);
            System.out.println("Copy with first attribute set to 0: " + copy);
            copy.setValue(weight, 1);
            System.out.println("Copy with weight attribute set to 1: " + copy);
            copy.setValue(position, "second");
            System.out.println("Copy with position set to \"second\": " + copy);
            copy.setValue(2, "first");
            System.out.println("Copy with last attribute set to \"first\": " + copy);
            System.out.println("Current weight of instance copy: " + copy.weight());
            copy.setWeight(2);
            System.out.println("Current weight of instance copy (set to 2): "
                    + copy.weight());
            System.out.println("Last value of copy: " + copy.toString(2));
            System.out.println("Value of position for copy: "
                    + copy.toString(position));
            System.out.println("Last value of copy (internal format): "
                    + copy.value(2));
            System.out.println("Value of position for copy (internal format): "
                    + copy.value(position));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Instances relationalValue(Attribute att) {
        int attIndex = att.index();
        if (att.isRelationValued()) {
            if (isMissing(attIndex)) {
                return null;
            }
//            return att.relation((int) value(attIndex));
            //todo fix the above line, for now we return null since we don't handle relations
            return null;
        } else {
            throw new IllegalArgumentException("Attribute isn't relation-valued!");
        }
    }
}
