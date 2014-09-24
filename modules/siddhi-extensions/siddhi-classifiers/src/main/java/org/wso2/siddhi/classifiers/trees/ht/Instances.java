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
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.core.util.SiddhiEnumeration;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.FileReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.*;

/**
 * Class for handling an ordered set of weighted instances.
 * <p/>
 * <p/>
 * Typical usage:
 * <p/>
 * <p/>
 * <pre>
 * import weka.core.converters.ConverterUtils.DataSource;
 * ...
 *
 * // Read all the instances in the file (ARFF, CSV, XRFF, ...)
 * DataSource source = new DataSource(filename);
 * Instances instances = source.getDataSet();
 *
 * // Make the last attribute be the class
 * instances.setClassIndex(instances.numAttributes() - 1);
 *
 * // Print header and instances.
 * System.out.println("\nDataset:\n");
 * System.out.println(instances);
 *
 * ...
 * </pre>
 * <p/>
 * <p/>
 * All methods that change a set of instances are safe, ie. a change of a set of
 * instances does not affect any other sets of instances. All methods that
 * change a datasets's attribute information clone the dataset before it is
 * changed.
 */
public class Instances extends AbstractList<Instance> implements Serializable {

    /**
     * for serialization
     */
    static final long serialVersionUID = -19412345060742748L;

    /**
     * The filename extension that should be used for arff files
     */
    public final static String FILE_EXTENSION = ".arff";

    /**
     * The filename extension that should be used for bin. serialized instances
     * files
     */
    public final static String SERIALIZED_OBJ_FILE_EXTENSION = ".bsi";

    /**
     * The keyword used to denote the start of an arff header
     */
    public final static String ARFF_RELATION = "@relation";

    /**
     * The keyword used to denote the start of the arff data section
     */
    public final static String ARFF_DATA = "@data";

    /**
     * The dataset's name.
     */
    protected/* @spec_public non_null@ */ String m_RelationName;

    /**
     * The attribute information.
     */
    protected/* @spec_public non_null@ */ ArrayList<Attribute> m_Attributes;
  /*
   * public invariant (\forall int i; 0 <= i && i < m_Attributes.size();
   * m_Attributes.get(i) != null);
   */

    /**
     * The instances.
     */
    protected/* @spec_public non_null@ */ ArrayList<Instance> m_Instances;

    /**
     * The class attribute's index
     */
    protected int m_ClassIndex;
    // @ protected invariant classIndex() == m_ClassIndex;

    protected int m_Lines = 0;


    /**
     * Constructor copying all instances and references to the header information
     * from the given set of instances.
     *
     * @param dataset the set to be copied
     */
    public Instances(/* @non_null@ */Instances dataset) {

        this(dataset, dataset.numInstances());

        dataset.copyInstances(0, this, dataset.numInstances());
    }

    /**
     * Constructor creating an empty set of instances. Copies references to the
     * header information from the given set of instances. Sets the capacity of
     * the set of instances to 0 if its negative.
     *
     * @param dataset  the instances from which the header information is to be
     *                 taken
     * @param capacity the capacity of the new dataset
     */
    public Instances(/* @non_null@ */Instances dataset, int capacity) {
        initialize(dataset, capacity);
    }

    /**
     * initializes with the header information of the given dataset and sets the
     * capacity of the set of instances.
     *
     * @param dataset  the dataset to use as template
     * @param capacity the number of rows to reserve
     */
    protected void initialize(Instances dataset, int capacity) {
        if (capacity < 0) {
            capacity = 0;
        }

        // Strings only have to be "shallow" copied because
        // they can't be modified.
        m_ClassIndex = dataset.m_ClassIndex;
        m_RelationName = dataset.m_RelationName;
        m_Attributes = dataset.m_Attributes;
        m_Instances = new ArrayList<Instance>(capacity);
    }

    /**
     * Creates a new set of instances by copying a subset of another set.
     *
     * @param source the set of instances from which a subset is to be created
     * @param first  the index of the first instance to be copied
     * @param toCopy the number of instances to be copied
     * @throws IllegalArgumentException if first and toCopy are out of range
     */
    // @ requires 0 <= first;
    // @ requires 0 <= toCopy;
    // @ requires first + toCopy <= source.numInstances();
    public Instances(/* @non_null@ */Instances source, int first, int toCopy) {

        this(source, toCopy);

        if ((first < 0) || ((first + toCopy) > source.numInstances())) {
            throw new IllegalArgumentException("Parameters first and/or toCopy out "
                    + "of range");
        }
        source.copyInstances(first, this, toCopy);
    }

    /**
     * Creates an empty set of instances. Uses the given attribute information.
     * Sets the capacity of the set of instances to 0 if its negative. Given
     * attribute information must not be changed after this constructor has been
     * used.
     *
     * @param name     the name of the relation
     * @param attInfo  the attribute information
     * @param capacity the capacity of the set
     * @throws IllegalArgumentException if attribute names are not unique
     */
    public Instances(/* @non_null@ */String name,
  /* @non_null@ */ArrayList<Attribute> attInfo, int capacity) {

        // check whether the attribute names are unique
        HashSet<String> names = new HashSet<String>();
        StringBuffer nonUniqueNames = new StringBuffer();
        for (Attribute att : attInfo) {
            if (names.contains(att.getName())) {
                nonUniqueNames.append("'" + att.getName() + "' ");
            }
            names.add(att.getName());
        }
        if (names.size() != attInfo.size()) {
            throw new IllegalArgumentException("Attribute names are not unique!"
                    + " Causes: " + nonUniqueNames.toString());
        }
        names.clear();

        m_RelationName = name;
        m_ClassIndex = -1;
        m_Attributes = attInfo;
        for (int i = 0; i < numAttributes(); i++) {
            attribute(i).setIndex(i);
        }
        m_Instances = new ArrayList<Instance>(capacity);
    }


    /**
     * Adds one instance to the end of the set. Shallow copies instance before it
     * is added. Increases the size of the dataset if it is not large enough. Does
     * not check if the instance is compatible with the dataset. Note: String or
     * relational values are not transferred.
     *
     * @param instance the instance to be added
     */
    @Override
    public boolean add(/* @non_null@ */Instance instance) {

        Instance newInstance = (Instance) instance.copy();

        newInstance.setDataset(this);
        m_Instances.add(newInstance);

        return true;
    }

    /**
     * Adds one instance at the given position in the list. Shallow
     * copies instance before it is added. Increases the size of the
     * dataset if it is not large enough. Does not check if the instance
     * is compatible with the dataset. Note: String or relational values
     * are not transferred.
     *
     * @param index    position where instance is to be inserted
     * @param instance the instance to be added
     */
    // @ requires 0 <= index;
    // @ requires index < m_Instances.size();
    @Override
    public void add(int index, /* @non_null@ */Instance instance) {

        Instance newInstance = (Instance) instance.copy();

        newInstance.setDataset(this);
        m_Instances.add(index, newInstance);
    }

    /**
     * Returns an attribute.
     *
     * @param index the attribute's index (index starts with 0)
     * @return the attribute at the given position
     */
    // @ requires 0 <= index;
    // @ requires index < m_Attributes.size();
    // @ ensures \result != null;
    public/* @pure@ */Attribute attribute(int index) {

        return m_Attributes.get(index);
    }

    /**
     * Returns an attribute given its name. If there is more than one attribute
     * with the same name, it returns the first one. Returns null if the attribute
     * can't be found.
     *
     * @param name the attribute's name
     * @return the attribute with the given name, null if the attribute can't be
     * found
     */
    public/* @pure@ */Attribute attribute(String name) {

        for (int i = 0; i < numAttributes(); i++) {
            if (attribute(i).getName().equals(name)) {
                return attribute(i);
            }
        }
        return null;
    }

    /**
     * Checks for attributes of the given type in the dataset
     *
     * @param attType the attribute type to look for
     * @return true if attributes of the given type are present
     */
    public boolean checkForAttributeType(int attType) {

        int i = 0;

        while (i < m_Attributes.size()) {
            if (attribute(i++).getType().ordinal() == attType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks for string attributes in the dataset
     *
     * @return true if string attributes are present, false otherwise
     */
    public/* @pure@ */boolean checkForStringAttributes() {
        return checkForAttributeType(Attribute.Type.STRING.ordinal());
    }

    /**
     * Checks if the given instance is compatible with this dataset. Only looks at
     * the size of the instance and the ranges of the values for nominal and
     * string attributes.
     *
     * @param instance the instance to check
     * @return true if the instance is compatible with the dataset
     */
    public/* @pure@ */boolean checkInstance(Instance instance) {

        if (instance.numAttributes() != numAttributes()) {
            return false;
        }
        for (int i = 0; i < numAttributes(); i++) {
            if (instance.isMissing(i)) {
                continue;
            } else if (attribute(i).isNominal() || attribute(i).isString()) {
                if (!(Utils.eq(instance.value(i), (int) instance.value(i)))) {
                    return false;
                } else if (Utils.sm(instance.value(i), 0)
                        || Utils.gr(instance.value(i), attribute(i).numValues())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the class attribute.
     *
     * @return the class attribute
     * @throws UnassignedClassException if the class is not set
     */
    // @ requires classIndex() >= 0;
    public/* @pure@ */Attribute classAttribute() {

        if (m_ClassIndex < 0) {
            throw new UnassignedClassException("Class index is negative (not set)!");
        }
        return attribute(m_ClassIndex);
    }

    /**
     * Returns the class attribute's index. Returns negative number if it's
     * undefined.
     *
     * @return the class index as an integer
     */
    // ensures \result == m_ClassIndex;
    public/* @pure@ */int classIndex() {

        return m_ClassIndex;
    }

    /**
     * Compactifies the set of instances. Decreases the capacity of the set so
     * that it matches the number of instances in the set.
     */
    public void compactify() {

        m_Instances.trimToSize();
    }

    /**
     * Removes all instances from the set.
     */
    public void delete() {

        m_Instances = new ArrayList<Instance>();
    }

    /**
     * Removes an instance at the given position from the set.
     *
     * @param index the instance's position (index starts with 0)
     */
    // @ requires 0 <= index && index < numInstances();
    public void delete(int index) {

        m_Instances.remove(index);
    }

    /**
     * Deletes an attribute at the given position (0 to numAttributes()
     * - 1). Attribute objects after the deletion point are copied so
     * that their indices can be decremented. Creates a fresh list to
     * hold the old and new attribute objects.
     *
     * @param position the attribute's position (position starts with 0)
     * @throws IllegalArgumentException if the given index is out of range or the
     *                                  class attribute is being deleted
     */
    // @ requires 0 <= position && position < numAttributes();
    // @ requires position != classIndex();
    public void deleteAttributeAt(int position) {

        if ((position < 0) || (position >= m_Attributes.size())) {
            throw new IllegalArgumentException("Index out of range");
        }
        if (position == m_ClassIndex) {
            throw new IllegalArgumentException("Can't delete class attribute");
        }

        ArrayList<Attribute> newList = new ArrayList<Attribute>(m_Attributes.size() - 1);
        newList.addAll(m_Attributes.subList(0, position));
        for (int i = position + 1; i < m_Attributes.size(); i++) {
            Attribute newAtt = (Attribute) m_Attributes.get(i).copy();
            newAtt.setIndex(i - 1);
            newList.add(newAtt);
        }
        m_Attributes = newList;

        if (m_ClassIndex > position) {
            m_ClassIndex--;
        }
        for (int i = 0; i < numInstances(); i++) {
            instance(i).setDataset(null);
            instance(i).deleteAttributeAt(position);
            instance(i).setDataset(this);
        }
    }

    /**
     * Deletes all attributes of the given type in the dataset. A deep copy of the
     * attribute information is performed before an attribute is deleted.
     *
     * @param attType the attribute type to delete
     * @throws IllegalArgumentException if attribute couldn't be successfully
     *                                  deleted (probably because it is the class attribute).
     */
    public void deleteAttributeType(int attType) {
        int i = 0;
        while (i < m_Attributes.size()) {
            if (attribute(i).getType().ordinal() == attType) {
                deleteAttributeAt(i);
            } else {
                i++;
            }
        }
    }

    /**
     * Deletes all string attributes in the dataset. A deep copy of the attribute
     * information is performed before an attribute is deleted.
     *
     * @throws IllegalArgumentException if string attribute couldn't be
     *                                  successfully deleted (probably because it is the class
     *                                  attribute).
     * @see #deleteAttributeType(int)
     */
    public void deleteStringAttributes() {
        deleteAttributeType(Attribute.Type.STRING.ordinal());
    }

    /**
     * Removes all instances with missing values for a particular attribute from
     * the dataset.
     *
     * @param attIndex the attribute's index (index starts with 0)
     */
    // @ requires 0 <= attIndex && attIndex < numAttributes();
    public void deleteWithMissing(int attIndex) {

        ArrayList<Instance> newInstances = new ArrayList<Instance>(numInstances());

        for (int i = 0; i < numInstances(); i++) {
            if (!instance(i).isMissing(attIndex)) {
                newInstances.add(instance(i));
            }
        }
        m_Instances = newInstances;
    }

    /**
     * Removes all instances with missing values for a particular attribute from
     * the dataset.
     *
     * @param att the attribute
     */
    public void deleteWithMissing(/* @non_null@ */Attribute att) {

        deleteWithMissing(att.getIndex());
    }

    /**
     * Removes all instances with a missing class value from the dataset.
     */
    public void deleteWithMissingClass() {

        if (m_ClassIndex < 0) {
            throw new UnassignedClassException("Class index is negative (not set)!");
        }
        deleteWithMissing(m_ClassIndex);
    }


    /**
     * Checks if two headers are equivalent. If not, then returns a message why
     * they differ.
     *
     * @param dataset another dataset
     * @return null if the header of the given dataset is equivalent to this
     * header, otherwise a message with details on why they differ
     */
    public String equalHeadersMsg(Instances dataset) {
        // Check class and all attributes
        if (m_ClassIndex != dataset.m_ClassIndex) {
            return "Class index differ: " + (m_ClassIndex + 1) + " != "
                    + (dataset.m_ClassIndex + 1);
        }

        if (m_Attributes.size() != dataset.m_Attributes.size()) {
            return "Different number of attributes: " + m_Attributes.size() + " != "
                    + dataset.m_Attributes.size();
        }

        for (int i = 0; i < m_Attributes.size(); i++) {
            if (dataset.attribute(i) == null) {
                return "Attributes differ at position " + (i + 1) + ":\n";
            }
        }

        return null;
    }

    /**
     * Checks if two headers are equivalent.
     *
     * @param dataset another dataset
     * @return true if the header of the given dataset is equivalent to this
     * header
     */
    public/* @pure@ */boolean equalHeaders(Instances dataset) {
        return (equalHeadersMsg(dataset) == null);
    }

    /**
     * Returns the first instance in the set.
     *
     * @return the first instance in the set
     */
    // @ requires numInstances() > 0;
    public/* @non_null pure@ */Instance firstInstance() {

        return m_Instances.get(0);
    }

    /**
     * Returns a random number generator. The initial seed of the random number
     * generator depends on the given seed and the hash code of a string
     * representation of a instances chosen based on the given seed.
     *
     * @param seed the given seed
     * @return the random number generator
     */
    public Random getRandomNumberGenerator(long seed) {

        Random r = new Random(seed);
        r.setSeed(instance(r.nextInt(numInstances())).toStringNoWeight().hashCode()
                + seed);
        return r;
    }

    /**
     * Inserts an attribute at the given position (0 to numAttributes())
     * and sets all values to be missing. Shallow copies the attribute
     * before it is inserted. Existing attribute objects at and after
     * the insertion point are also copied so that their indices can be
     * incremented. Creates a fresh list to hold the old and new
     * attribute objects.
     *
     * @param att      the attribute to be inserted
     * @param position the attribute's position (position starts with 0)
     * @throws IllegalArgumentException if the given index is out of range
     */
    // @ requires 0 <= position;
    // @ requires position <= numAttributes();
    public void insertAttributeAt(/* @non_null@ */Attribute att, int position) {

        if ((position < 0) || (position > m_Attributes.size())) {
            throw new IllegalArgumentException("Index out of range");
        }
        if (attribute(att.getName()) != null) {
            throw new IllegalArgumentException("Attribute name '" + att.getName()
                    + "' already in use at position #" + attribute(att.getName()).index());
        }
        att = (Attribute) att.copy();
        att.setIndex(position);

        ArrayList<Attribute> newList = new ArrayList<Attribute>(m_Attributes.size() + 1);
        newList.addAll(m_Attributes.subList(0, position));
        newList.add(att);
        for (int i = position; i < m_Attributes.size(); i++) {
            Attribute newAtt = (Attribute) m_Attributes.get(i).copy();
            newAtt.setIndex(i + 1);
            newList.add(newAtt);
        }
        m_Attributes = newList;

        for (int i = 0; i < numInstances(); i++) {
            instance(i).setDataset(null);
            instance(i).insertAttributeAt(position);
            instance(i).setDataset(this);
        }
        if (m_ClassIndex >= position) {
            m_ClassIndex++;
        }
    }

    /**
     * Returns the instance at the given position.
     *
     * @param index the instance's index (index starts with 0)
     * @return the instance at the given position
     */
    // @ requires 0 <= index;
    // @ requires index < numInstances();
    public/* @non_null pure@ */Instance instance(int index) {

        return m_Instances.get(index);
    }

    /**
     * Returns the instance at the given position.
     *
     * @param index the instance's index (index starts with 0)
     * @return the instance at the given position
     */
    // @ requires 0 <= index;
    // @ requires index < numInstances();
    @Override
    public/* @non_null pure@ */Instance get(int index) {

        return m_Instances.get(index);
    }

    /**
     * Returns the last instance in the set.
     *
     * @return the last instance in the set
     */
    // @ requires numInstances() > 0;
    public/* @non_null pure@ */Instance lastInstance() {

        return m_Instances.get(m_Instances.size() - 1);
    }


    /**
     * Returns the number of attributes.
     *
     * @return the number of attributes as an integer
     */
    // @ ensures \result == m_Attributes.size();
    public/* @pure@ */int numAttributes() {

        return m_Attributes.size();
    }

    /**
     * Returns the number of class labels.
     *
     * @return the number of class labels as an integer if the class attribute is
     * nominal, 1 otherwise.
     * @throws UnassignedClassException if the class is not set
     */
    // @ requires classIndex() >= 0;
    public/* @pure@ */int numClasses() {

        if (m_ClassIndex < 0) {
            throw new UnassignedClassException("Class index is negative (not set)!");
        }
        if (!classAttribute().isNominal()) {
            return 1;
        } else {
            return classAttribute().numValues();
        }
    }

    /**
     * Returns the number of distinct values of a given attribute. Returns the
     * number of instances if the attribute is a string attribute. The value
     * 'missing' is not counted.
     *
     * @param attIndex the attribute (index starts with 0)
     * @return the number of distinct values of a given attribute
     */
    // @ requires 0 <= attIndex;
    // @ requires attIndex < numAttributes();
    public/* @pure@ */int numDistinctValues(int attIndex) {

        if (attribute(attIndex).isNumeric()) {
            double[] attVals = attributeToDoubleArray(attIndex);
            int[] sorted = Utils.sort(attVals);
            double prev = 0;
            int counter = 0;
            for (int i = 0; i < sorted.length; i++) {
                Instance current = instance(sorted[i]);
                if (current.isMissing(attIndex)) {
                    break;
                }
                if ((i == 0) || (current.value(attIndex) > prev)) {
                    prev = current.value(attIndex);
                    counter++;
                }
            }
            return counter;
        } else {
            return attribute(attIndex).numValues();
        }
    }

    /**
     * Returns the number of distinct values of a given attribute. Returns the
     * number of instances if the attribute is a string attribute. The value
     * 'missing' is not counted.
     *
     * @param att the attribute
     * @return the number of distinct values of a given attribute
     */
    public/* @pure@ */int numDistinctValues(/* @non_null@ */Attribute att) {

        return numDistinctValues(att.getIndex());
    }

    /**
     * Returns the number of instances in the dataset.
     *
     * @return the number of instances in the dataset as an integer
     */
    // @ ensures \result == m_Instances.size();
    public/* @pure@ */int numInstances() {

        return m_Instances.size();
    }

    /**
     * Returns the number of instances in the dataset.
     *
     * @return the number of instances in the dataset as an integer
     */
    // @ ensures \result == m_Instances.size();
    @Override
    public/* @pure@ */int size() {

        return m_Instances.size();
    }

    /**
     * Shuffles the instances in the set so that they are ordered randomly.
     *
     * @param random a random number generator
     */
    public void randomize(Random random) {

        for (int j = numInstances() - 1; j > 0; j--) {
            swap(j, random.nextInt(j + 1));
        }
    }


    /**
     * Replaces the attribute at the given position (0 to
     * numAttributes()) with the given attribute and sets all its values to
     * be missing. Shallow copies the given attribute before it is
     * inserted. Creates a fresh list to hold the old and new
     * attribute objects.
     *
     * @param att      the attribute to be inserted
     * @param position the attribute's position (position starts with 0)
     * @throws IllegalArgumentException if the given index is out of range
     */
    // @ requires 0 <= position;
    // @ requires position <= numAttributes();
    public void replaceAttributeAt(/* @non_null@ */Attribute att, int position) {

        if ((position < 0) || (position > m_Attributes.size())) {
            throw new IllegalArgumentException("Index out of range");
        }

        // Does the new attribute have a different name?
        if (!att.getName().equals(m_Attributes.get(position).getName())) {

            // Need to check if attribute name already exists
            Attribute candidate = attribute(att.getName());
            if ((candidate != null) && (position != candidate.getIndex())) {
                throw new IllegalArgumentException("Attribute name '" + att.getName()
                        + "' already in use at position #" +
                        attribute(att.getName()).getIndex());
            }
        }
        att = (Attribute) att.copy();
        att.setIndex(position);

        ArrayList<Attribute> newList = new ArrayList<Attribute>(m_Attributes.size());
        newList.addAll(m_Attributes.subList(0, position));
        newList.add(att);
        newList.addAll(m_Attributes.subList(position + 1, m_Attributes.size()));
        m_Attributes = newList;

        for (int i = 0; i < numInstances(); i++) {
            instance(i).setDataset(null);
            instance(i).setMissing(position);
            instance(i).setDataset(this);
        }
        if (m_ClassIndex >= position) {
            m_ClassIndex++;
        }
    }

    /**
     * Returns the relation's name.
     *
     * @return the relation's name as a string
     */
    // @ ensures \result == m_RelationName;
    public/* @pure@ */String relationName() {

        return m_RelationName;
    }

    /**
     * Removes the instance at the given position.
     *
     * @param index the instance's index (index starts with 0)
     * @return the instance at the given position
     */
    // @ requires 0 <= index;
    // @ requires index < numInstances();
    @Override
    public Instance remove(int index) {

        return m_Instances.remove(index);
    }

    /**
     * Renames an attribute. This change only affects this dataset.
     *
     * @param att  the attribute's index (index starts with 0)
     * @param name the new name
     */
    public void renameAttribute(int att, String name) {
        // name already present?
        for (int i = 0; i < numAttributes(); i++) {
            if (i == att) {
                continue;
            }
            if (attribute(i).getName().equals(name)) {
                throw new IllegalArgumentException("Attribute name '" + name
                        + "' already present at position #" + i);
            }
        }

        Attribute newAtt = attribute(att).copy(name);
        ArrayList<Attribute> newVec = new ArrayList<Attribute>(numAttributes());
        for (Attribute attr : m_Attributes) {
            if (attr.getIndex() == att) {
                newVec.add(newAtt);
            } else {
                newVec.add(attr);
            }
        }
        m_Attributes = newVec;
    }

    /**
     * Renames an attribute. This change only affects this dataset.
     *
     * @param att  the attribute
     * @param name the new name
     */
    public void renameAttribute(Attribute att, String name) {

        renameAttribute(att.getIndex(), name);
    }

    /**
     * Renames the value of a nominal (or string) attribute value. This change
     * only affects this dataset.
     *
     * @param att  the attribute's index (index starts with 0)
     * @param val  the value's index (index starts with 0)
     * @param name the new name
     */
    public void renameAttributeValue(int att, int val, String name) {

        Attribute newAtt = (Attribute) attribute(att).copy();
        ArrayList<Attribute> newVec = new ArrayList<Attribute>(numAttributes());

        newAtt.setValue(val, name);
        for (Attribute attr : m_Attributes) {
            if (attr.getIndex() == att) {
                newVec.add(newAtt);
            } else {
                newVec.add(attr);
            }
        }
        m_Attributes = newVec;
    }

    /**
     * Renames the value of a nominal (or string) attribute value. This change
     * only affects this dataset.
     *
     * @param att  the attribute
     * @param val  the value
     * @param name the new name
     */
    public void renameAttributeValue(Attribute att, String val, String name) {

        int v = att.indexOfValue(val);
        if (v == -1) {
            throw new IllegalArgumentException(val + " not found");
        }
        renameAttributeValue(att.index(), v, name);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement.
     *
     * @param random a random number generator
     * @return the new dataset
     */
    public Instances resample(Random random) {

        Instances newData = new Instances(this, numInstances());
        while (newData.numInstances() < numInstances()) {
            newData.add(instance(random.nextInt(numInstances())));
        }
        return newData;
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the current instance weights. The weights of the
     * instances in the new dataset are set to one. See also
     * resampleWithWeights(Random, double[], boolean[]).
     *
     * @param random a random number generator
     * @return the new dataset
     */
    public Instances resampleWithWeights(Random random) {

        return resampleWithWeights(random, false);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the current instance weights. The weights of the
     * instances in the new dataset are set to one. See also
     * resampleWithWeights(Random, double[], boolean[]).
     *
     * @param random  a random number generator
     * @param sampled an array indicating what has been sampled
     * @return the new dataset
     */
    public Instances resampleWithWeights(Random random, boolean[] sampled) {

        return resampleWithWeights(random, sampled, false);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the current instance weights. The weights of the
     * instances in the new dataset are set to one. See also
     * resampleWithWeights(Random, double[], boolean[]).
     *
     * @param random                a random number generator
     * @param representUsingWeights if true, copies are represented using weights
     *                              in resampled data
     * @return the new dataset
     */
    public Instances resampleWithWeights(Random random,
                                         boolean representUsingWeights) {

        return resampleWithWeights(random, null, representUsingWeights);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the current instance weights. The weights of the
     * instances in the new dataset are set to one. See also
     * resampleWithWeights(Random, double[], boolean[]).
     *
     * @param random                a random number generator
     * @param sampled               an array indicating what has been sampled
     * @param representUsingWeights if true, copies are represented using weights
     *                              in resampled data
     * @return the new dataset
     */
    public Instances resampleWithWeights(Random random, boolean[] sampled,
                                         boolean representUsingWeights) {

        double[] weights = new double[numInstances()];
        for (int i = 0; i < weights.length; i++) {
            weights[i] = instance(i).weight();
        }
        return resampleWithWeights(random, weights, sampled, representUsingWeights);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the given weight vector. See also
     * resampleWithWeights(Random, double[], boolean[]).
     *
     * @param random  a random number generator
     * @param weights the weight vector
     * @return the new dataset
     * @throws IllegalArgumentException if the weights array is of the wrong
     *                                  length or contains negative weights.
     */
    public Instances resampleWithWeights(Random random, double[] weights) {

        return resampleWithWeights(random, weights, null);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the given weight vector. The weights of the
     * instances in the new dataset are set to one. The length of the weight
     * vector has to be the same as the number of instances in the dataset, and
     * all weights have to be positive. Uses Walker's method, see pp. 232 of
     * "Stochastic Simulation" by B.D. Ripley (1987).
     *
     * @param random  a random number generator
     * @param weights the weight vector
     * @param sampled an array indicating what has been sampled, can be null
     * @return the new dataset
     * @throws IllegalArgumentException if the weights array is of the wrong
     *                                  length or contains negative weights.
     */
    public Instances resampleWithWeights(Random random, double[] weights,
                                         boolean[] sampled) {

        return resampleWithWeights(random, weights, sampled, false);
    }

    /**
     * Creates a new dataset of the same size using random sampling with
     * replacement according to the given weight vector. The weights of the
     * instances in the new dataset are set to one. The length of the weight
     * vector has to be the same as the number of instances in the dataset, and
     * all weights have to be positive. Uses Walker's method, see pp. 232 of
     * "Stochastic Simulation" by B.D. Ripley (1987).
     *
     * @param random                a random number generator
     * @param weights               the weight vector
     * @param sampled               an array indicating what has been sampled, can be null
     * @param representUsingWeights if true, copies are represented using weights
     *                              in resampled data
     * @return the new dataset
     * @throws IllegalArgumentException if the weights array is of the wrong
     *                                  length or contains negative weights.
     */
    public Instances resampleWithWeights(Random random, double[] weights,
                                         boolean[] sampled, boolean representUsingWeights) {

        if (weights.length != numInstances()) {
            throw new IllegalArgumentException("weights.length != numInstances.");
        }

        Instances newData = new Instances(this, numInstances());
        if (numInstances() == 0) {
            return newData;
        }

        // Walker's method, see pp. 232 of "Stochastic Simulation" by B.D. Ripley
        double[] P = new double[weights.length];
        System.arraycopy(weights, 0, P, 0, weights.length);
        Utils.normalize(P);
        double[] Q = new double[weights.length];
        int[] A = new int[weights.length];
        int[] W = new int[weights.length];
        int M = weights.length;
        int NN = -1;
        int NP = M;
        for (int I = 0; I < M; I++) {
            if (P[I] < 0) {
                throw new IllegalArgumentException("Weights have to be positive.");
            }
            Q[I] = M * P[I];
            if (Q[I] < 1.0) {
                W[++NN] = I;
            } else {
                W[--NP] = I;
            }
        }
        if (NN > -1 && NP < M) {
            for (int S = 0; S < M - 1; S++) {
                int I = W[S];
                int J = W[NP];
                A[I] = J;
                Q[J] += Q[I] - 1.0;
                if (Q[J] < 1.0) {
                    NP++;
                }
                if (NP >= M) {
                    break;
                }
            }
            // A[W[M]] = W[M];
        }

        for (int I = 0; I < M; I++) {
            Q[I] += I;
        }

        // Do we need to keep track of how many copies to use?
        int[] counts = null;
        if (representUsingWeights) {
            counts = new int[M];
        }

        for (int i = 0; i < numInstances(); i++) {
            int ALRV;
            double U = M * random.nextDouble();
            int I = (int) U;
            if (U < Q[I]) {
                ALRV = I;
            } else {
                ALRV = A[I];
            }
            if (representUsingWeights) {
                counts[ALRV]++;
            } else {
                newData.add(instance(ALRV));
            }
            if (sampled != null) {
                sampled[ALRV] = true;
            }
            if (!representUsingWeights) {
                newData.instance(newData.numInstances() - 1).setWeight(1);
            }
        }

        // Add data based on counts if weights should represent numbers of copies.
        if (representUsingWeights) {
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    newData.add(instance(i));
                    newData.instance(newData.numInstances() - 1).setWeight(counts[i]);
                }
            }
        }

        return newData;
    }

    /**
     * Replaces the instance at the given position. Shallow copies instance before
     * it is added. Does not check if the instance is compatible with the dataset.
     * Note: String or relational values are not transferred.
     *
     * @param index    position where instance is to be inserted
     * @param instance the instance to be inserted
     * @return the instance previously at that position
     */
    // @ requires 0 <= index;
    // @ requires index < m_Instances.size();
    @Override
    public Instance set(int index, /* @non_null@ */Instance instance) {

        Instance newInstance = (Instance) instance.copy();
        Instance oldInstance = m_Instances.get(index);

        newInstance.setDataset(this);
        m_Instances.set(index, newInstance);

        return oldInstance;
    }

    /**
     * Sets the class attribute.
     *
     * @param att attribute to be the class
     */
    public void setClass(Attribute att) {

        m_ClassIndex = att.getIndex();
    }

    /**
     * Sets the class index of the set. If the class index is negative there is
     * assumed to be no class. (ie. it is undefined)
     *
     * @param classIndex the new class index (index starts with 0)
     * @throws IllegalArgumentException if the class index is too big or < 0
     */
    public void setClassIndex(int classIndex) {

        if (classIndex >= numAttributes()) {
            throw new IllegalArgumentException("Invalid class index: " + classIndex);
        }
        m_ClassIndex = classIndex;
    }

    /**
     * Sets the relation's name.
     *
     * @param newName the new relation name.
     */
    public void setRelationName(/* @non_null@ */String newName) {

        m_RelationName = newName;
    }

    /**
     * Sorts the instances based on an attribute. For numeric attributes,
     * instances are sorted in ascending order. For nominal attributes, instances
     * are sorted based on the attribute label ordering specified in the header.
     * Instances with missing values for the attribute are placed at the end of
     * the dataset.
     *
     * @param attIndex the attribute's index (index starts with 0)
     */
    public void sort(int attIndex) {

        double[] vals = new double[numInstances()];
        Instance[] backup = new Instance[vals.length];
        for (int i = 0; i < vals.length; i++) {
            Instance inst = instance(i);
            backup[i] = inst;
            double val = inst.value(attIndex);
            if (Utils.isMissingValue(val)) {
                vals[i] = Double.MAX_VALUE;
            } else {
                vals[i] = val;
            }
        }

        int[] sortOrder = Utils.sortWithNoMissingValues(vals);
        for (int i = 0; i < vals.length; i++) {
            m_Instances.set(i, backup[sortOrder[i]]);
        }
    }

    /**
     * Sorts the instances based on an attribute. For numeric attributes,
     * instances are sorted into ascending order. For nominal attributes,
     * instances are sorted based on the attribute label ordering specified in the
     * header. Instances with missing values for the attribute are placed at the
     * end of the dataset.
     *
     * @param att the attribute
     */
    public void sort(Attribute att) {

        sort(att.index());
    }

    /**
     * Stratifies a set of instances according to its class values if the class
     * attribute is nominal (so that afterwards a stratified cross-validation can
     * be performed).
     *
     * @param numFolds the number of folds in the cross-validation
     * @throws UnassignedClassException if the class is not set
     */
    public void stratify(int numFolds) {

        if (numFolds <= 1) {
            throw new IllegalArgumentException(
                    "Number of folds must be greater than 1");
        }
        if (m_ClassIndex < 0) {
            throw new UnassignedClassException("Class index is negative (not set)!");
        }
        if (classAttribute().isNominal()) {

            // sort by class
            int index = 1;
            while (index < numInstances()) {
                Instance instance1 = instance(index - 1);
                for (int j = index; j < numInstances(); j++) {
                    Instance instance2 = instance(j);
                    if ((instance1.classValue() == instance2.classValue())
                            || (instance1.classIsMissing() && instance2.classIsMissing())) {
                        swap(index, j);
                        index++;
                    }
                }
                index++;
            }
            stratStep(numFolds);
        }
    }

    /**
     * Computes the sum of all the instances' weights.
     *
     * @return the sum of all the instances' weights as a double
     */
    public/* @pure@ */double sumOfWeights() {

        double sum = 0;

        for (int i = 0; i < numInstances(); i++) {
            sum += instance(i).weight();
        }
        return sum;
    }

    /**
     * Creates the test set for one fold of a cross-validation on the dataset.
     *
     * @param numFolds the number of folds in the cross-validation. Must be
     *                 greater than 1.
     * @param numFold  0 for the first fold, 1 for the second, ...
     * @return the test set as a set of weighted instances
     * @throws IllegalArgumentException if the number of folds is less than 2 or
     *                                  greater than the number of instances.
     */
    // @ requires 2 <= numFolds && numFolds < numInstances();
    // @ requires 0 <= numFold && numFold < numFolds;
    public Instances testCV(int numFolds, int numFold) {

        int numInstForFold, first, offset;
        Instances test;

        if (numFolds < 2) {
            throw new IllegalArgumentException("Number of folds must be at least 2!");
        }
        if (numFolds > numInstances()) {
            throw new IllegalArgumentException(
                    "Can't have more folds than instances!");
        }
        numInstForFold = numInstances() / numFolds;
        if (numFold < numInstances() % numFolds) {
            numInstForFold++;
            offset = numFold;
        } else {
            offset = numInstances() % numFolds;
        }
        test = new Instances(this, numInstForFold);
        first = numFold * (numInstances() / numFolds) + offset;
        copyInstances(first, test, numInstForFold);
        return test;
    }

    /**
     * Returns the dataset as a string in ARFF format. Strings are quoted if they
     * contain whitespace characters, or if they are a question mark.
     *
     * @return the dataset in ARFF format as a string
     */
    @Override
    public String toString() {

        StringBuffer text = new StringBuffer();

        text.append(ARFF_RELATION).append(" ").append(Utils.quote(m_RelationName))
                .append("\n\n");
        for (int i = 0; i < numAttributes(); i++) {
            text.append(attribute(i)).append("\n");
        }
        text.append("\n").append(ARFF_DATA).append("\n");

        text.append(stringWithoutHeader());
        return text.toString();
    }

    /**
     * Returns the instances in the dataset as a string in ARFF format. Strings
     * are quoted if they contain whitespace characters, or if they are a question
     * mark.
     *
     * @return the dataset in ARFF format as a string
     */
    protected String stringWithoutHeader() {

        StringBuffer text = new StringBuffer();

        for (int i = 0; i < numInstances(); i++) {
            text.append(instance(i));
            if (i < numInstances() - 1) {
                text.append('\n');
            }
        }
        return text.toString();
    }

    /**
     * Creates the training set for one fold of a cross-validation on the dataset.
     *
     * @param numFolds the number of folds in the cross-validation. Must be
     *                 greater than 1.
     * @param numFold  0 for the first fold, 1 for the second, ...
     * @return the training set
     * @throws IllegalArgumentException if the number of folds is less than 2 or
     *                                  greater than the number of instances.
     */
    // @ requires 2 <= numFolds && numFolds < numInstances();
    // @ requires 0 <= numFold && numFold < numFolds;
    public Instances trainCV(int numFolds, int numFold) {

        int numInstForFold, first, offset;
        Instances train;

        if (numFolds < 2) {
            throw new IllegalArgumentException("Number of folds must be at least 2!");
        }
        if (numFolds > numInstances()) {
            throw new IllegalArgumentException(
                    "Can't have more folds than instances!");
        }
        numInstForFold = numInstances() / numFolds;
        if (numFold < numInstances() % numFolds) {
            numInstForFold++;
            offset = numFold;
        } else {
            offset = numInstances() % numFolds;
        }
        train = new Instances(this, numInstances() - numInstForFold);
        first = numFold * (numInstances() / numFolds) + offset;
        copyInstances(0, train, first);
        copyInstances(first + numInstForFold, train, numInstances() - first
                - numInstForFold);

        return train;
    }

    /**
     * Creates the training set for one fold of a cross-validation on the dataset.
     * The data is subsequently randomized based on the given random number
     * generator.
     *
     * @param numFolds the number of folds in the cross-validation. Must be
     *                 greater than 1.
     * @param numFold  0 for the first fold, 1 for the second, ...
     * @param random   the random number generator
     * @return the training set
     * @throws IllegalArgumentException if the number of folds is less than 2 or
     *                                  greater than the number of instances.
     */
    // @ requires 2 <= numFolds && numFolds < numInstances();
    // @ requires 0 <= numFold && numFold < numFolds;
    public Instances trainCV(int numFolds, int numFold, Random random) {

        Instances train = trainCV(numFolds, numFold);
        train.randomize(random);
        return train;
    }

    /**
     * Computes the variance for a numeric attribute.
     *
     * @param attIndex the numeric attribute (index starts with 0)
     * @return the variance if the attribute is numeric
     * @throws IllegalArgumentException if the attribute is not numeric
     */
    public/* @pure@ */double variance(int attIndex) {

        double sum = 0, sumSquared = 0, sumOfWeights = 0;

        if (!attribute(attIndex).isNumeric()) {
            throw new IllegalArgumentException(
                    "Can't compute variance because attribute is " + "not numeric!");
        }
        for (int i = 0; i < numInstances(); i++) {
            if (!instance(i).isMissing(attIndex)) {
                sum += instance(i).weight() * instance(i).value(attIndex);
                sumSquared += instance(i).weight() * instance(i).value(attIndex)
                        * instance(i).value(attIndex);
                sumOfWeights += instance(i).weight();
            }
        }
        if (sumOfWeights <= 1) {
            return 0;
        }
        double result = (sumSquared - (sum * sum / sumOfWeights))
                / (sumOfWeights - 1);

        // We don't like negative variance
        if (result < 0) {
            return 0;
        } else {
            return result;
        }
    }

    /**
     * Computes the variance for a numeric attribute.
     *
     * @param att the numeric attribute
     * @return the variance if the attribute is numeric
     * @throws IllegalArgumentException if the attribute is not numeric
     */
    public/* @pure@ */double variance(Attribute att) {

        return variance(att.index());
    }

    /**
     * Calculates summary statistics on the values that appear in this set of
     * instances for a specified attribute.
     *
     * @param index the index of the attribute to summarize (index starts with 0)
     * @return an AttributeStats object with it's fields calculated.
     */
    // @ requires 0 <= index && index < numAttributes();
    public AttributeStats attributeStats(int index) {

        AttributeStats result = new AttributeStats();
        if (attribute(index).isNominal()) {
            result.nominalCounts = new int[attribute(index).numValues()];
            result.nominalWeights = new double[attribute(index).numValues()];
        }
        if (attribute(index).isNumeric()) {
            result.numericStats = new Stats();
        }
        result.totalCount = numInstances();

        double[] attVals = attributeToDoubleArray(index);
        int[] sorted = Utils.sort(attVals);
        int currentCount = 0;
        double currentWeight = 0;
        double prev = Double.NaN;
        for (int j = 0; j < numInstances(); j++) {
            Instance current = instance(sorted[j]);
            if (current.isMissing(index)) {
                result.missingCount = numInstances() - j;
                break;
            }
            if (current.value(index) == prev) {
                currentCount++;
                currentWeight += current.weight();
            } else {
                result.addDistinct(prev, currentCount, currentWeight);
                currentCount = 1;
                currentWeight = current.weight();
                prev = current.value(index);
            }
        }
        result.addDistinct(prev, currentCount, currentWeight);
        result.distinctCount--; // So we don't count "missing" as a value
        return result;
    }

    /**
     * Gets the value of all instances in this dataset for a particular attribute.
     * Useful in conjunction with Utils.sort to allow iterating through the
     * dataset in sorted order for some attribute.
     *
     * @param index the index of the attribute.
     * @return an array containing the value of the desired attribute for each
     * instance in the dataset.
     */
    // @ requires 0 <= index && index < numAttributes();
    public/* @pure@ */double[] attributeToDoubleArray(int index) {

        double[] result = new double[numInstances()];
        for (int i = 0; i < result.length; i++) {
            result[i] = instance(i).value(index);
        }
        return result;
    }

    /**
     * Generates a string summarizing the set of instances. Gives a breakdown for
     * each attribute indicating the number of missing/discrete/unique values and
     * other information.
     *
     * @return a string summarizing the dataset
     */
    public String toSummaryString() {

        StringBuffer result = new StringBuffer();
        result.append("Relation Name:  ").append(relationName()).append('\n');
        result.append("Num Instances:  ").append(numInstances()).append('\n');
        result.append("Num Attributes: ").append(numAttributes()).append('\n');
        result.append('\n');

        result.append(Utils.padLeft("", 5)).append(Utils.padRight("Name", 25));
        result.append(Utils.padLeft("Type", 5)).append(Utils.padLeft("Nom", 5));
        result.append(Utils.padLeft("Int", 5)).append(Utils.padLeft("Real", 5));
        result.append(Utils.padLeft("Missing", 12));
        result.append(Utils.padLeft("Unique", 12));
        result.append(Utils.padLeft("Dist", 6)).append('\n');

        // Figure out how many digits we need for the index
        int numDigits = (int) Math.log10((int) numAttributes()) + 1;

        for (int i = 0; i < numAttributes(); i++) {
            Attribute a = attribute(i);
            AttributeStats as = attributeStats(i);
            result.append(Utils.padLeft("" + (i + 1), numDigits)).append(' ');
            result.append(Utils.padRight(a.getName(), 25)).append(' ');
            long percent;
            switch (a.getType()) {
                case NOMINAL:
                    result.append(Utils.padLeft("Nom", 4)).append(' ');
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
                case NUMERIC:
                    result.append(Utils.padLeft("Num", 4)).append(' ');
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
                case DATE:
                    result.append(Utils.padLeft("Dat", 4)).append(' ');
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
                case STRING:
                    result.append(Utils.padLeft("Str", 4)).append(' ');
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
                case RELATIONAL:
                    result.append(Utils.padLeft("Rel", 4)).append(' ');
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
                default:
                    result.append(Utils.padLeft("???", 4)).append(' ');
                    result.append(Utils.padLeft("" + 0, 3)).append("% ");
                    percent = Math.round(100.0 * as.intCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    percent = Math.round(100.0 * as.realCount / as.totalCount);
                    result.append(Utils.padLeft("" + percent, 3)).append("% ");
                    break;
            }
            result.append(Utils.padLeft("" + as.missingCount, 5)).append(" /");
            percent = Math.round(100.0 * as.missingCount / as.totalCount);
            result.append(Utils.padLeft("" + percent, 3)).append("% ");
            result.append(Utils.padLeft("" + as.uniqueCount, 5)).append(" /");
            percent = Math.round(100.0 * as.uniqueCount / as.totalCount);
            result.append(Utils.padLeft("" + percent, 3)).append("% ");
            result.append(Utils.padLeft("" + as.distinctCount, 5)).append(' ');
            result.append('\n');
        }
        return result.toString();
    }

    /**
     * Copies instances from one set to the end of another one.
     *
     * @param from the position of the first instance to be copied
     * @param dest the destination for the instances
     * @param num  the number of instances to be copied
     */
    // @ requires 0 <= from && from <= numInstances() - num;
    // @ requires 0 <= num;
    protected void copyInstances(int from, /* @non_null@ */Instances dest, int num) {

        for (int i = 0; i < num; i++) {
            dest.add(instance(from + i));
        }
    }

    /**
     * Replaces the attribute information by a clone of itself.
     */
    protected void freshAttributeInfo() {

        ArrayList<Attribute> newList = new ArrayList<Attribute>(m_Attributes.size());
        for (Attribute att : m_Attributes) {
            newList.add((Attribute) att.copy());
        }
        m_Attributes = newList;
    }

    /**
     * Returns string including all instances, their weights and their indices in
     * the original dataset.
     *
     * @return description of instance and its weight as a string
     */
    protected/* @pure@ */String instancesAndWeights() {

        StringBuffer text = new StringBuffer();

        for (int i = 0; i < numInstances(); i++) {
            text.append(instance(i) + " " + instance(i).weight());
            if (i < numInstances() - 1) {
                text.append("\n");
            }
        }
        return text.toString();
    }

    /**
     * Help function needed for stratification of set.
     *
     * @param numFolds the number of folds for the stratification
     */
    protected void stratStep(int numFolds) {

        ArrayList<Instance> newVec = new ArrayList<Instance>(m_Instances.size());
        int start = 0, j;

        // create stratified batch
        while (newVec.size() < numInstances()) {
            j = start;
            while (j < numInstances()) {
                newVec.add(instance(j));
                j = j + numFolds;
            }
            start++;
        }
        m_Instances = newVec;
    }

    /**
     * Swaps two instances in the set.
     *
     * @param i the first instance's index (index starts with 0)
     * @param j the second instance's index (index starts with 0)
     */
    // @ requires 0 <= i && i < numInstances();
    // @ requires 0 <= j && j < numInstances();
    public void swap(int i, int j) {

        Instance in = m_Instances.get(i);
        m_Instances.set(i, m_Instances.get(j));
        m_Instances.set(j, in);
    }

    /**
     * Merges two sets of Instances together. The resulting set will have all the
     * attributes of the first set plus all the attributes of the second set. The
     * number of instances in both sets must be the same.
     *
     * @param first  the first set of Instances
     * @param second the second set of Instances
     * @return the merged set of Instances
     * @throws IllegalArgumentException if the datasets are not the same size
     */
    public static Instances mergeInstances(Instances first, Instances second) {

        if (first.numInstances() != second.numInstances()) {
            throw new IllegalArgumentException(
                    "Instance sets must be of the same size");
        }

        // Create the vector of merged attributes
        ArrayList<Attribute> newAttributes = new ArrayList<Attribute>();
        for (Attribute att : first.m_Attributes) {
            newAttributes.add(att);
        }
        for (Attribute att : second.m_Attributes) {
            newAttributes.add((Attribute) att.copy()); // Need to copy because indices will change.
        }

        // Create the set of Instances
        Instances merged = new Instances(first.relationName() + '_'
                + second.relationName(), newAttributes, first.numInstances());
        // Merge each instance
        for (int i = 0; i < first.numInstances(); i++) {
            merged.add(first.instance(i).mergeInstance(second.instance(i)));
        }
        return merged;
    }

    public/* @non_null pure@ */Enumeration<Attribute> enumerateAttributes() {

        return new SiddhiEnumeration<Attribute>(m_Attributes, m_ClassIndex);
    }

    /**
     * Returns an enumeration of all instances in the dataset.
     *
     * @return enumeration of all instances in the dataset
     */
    public/* @non_null pure@ */Enumeration<Instance> enumerateInstances() {

        return new SiddhiEnumeration<Instance>(m_Instances);
    }
}

