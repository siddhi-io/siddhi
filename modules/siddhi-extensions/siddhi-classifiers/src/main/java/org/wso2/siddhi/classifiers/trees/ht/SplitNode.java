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

import org.wso2.siddhi.classifiers.trees.ht.nodes.HNode;
import org.wso2.siddhi.classifiers.trees.ht.nodes.LeafNode;

import java.util.LinkedHashMap;
import java.util.Map;

public class SplitNode extends HNode {

    /**
     * For serialization
     */
    private static final long serialVersionUID = 1558033628618451073L;

    /**
     * The split itself
     */
    protected Split m_split;

    /**
     * Child nodes
     */
    protected Map<String, HNode> m_children = new LinkedHashMap<String, HNode>();

    /**
     * Construct a new SplitNode
     *
     * @param classDistrib the class distribution
     * @param split        the split
     */
    public SplitNode(Map<String, WeightMass> classDistrib, Split split) {
        super(classDistrib);

        m_split = split;
    }

    /**
     * Return the branch that the supplied instance goes down
     *
     * @param inst the instance to find the branch for
     * @return the branch that the supplied instance goes down
     */
    public String branchForInstance(Instance inst) {
        return m_split.branchForInstance(inst);
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Number of child nodes
     *
     * @return the number of child nodes
     */
    public int numChildred() {
        return m_children.size();
    }

    /**
     * Add a child
     *
     * @param branch the branch for the child
     * @param child  the child itself
     */
    public void setChild(String branch, HNode child) {
        m_children.put(branch, child);
    }

    @Override
    public LeafNode leafForInstance(Instance inst, SplitNode parent,
                                    String parentBranch) {

        String branch = branchForInstance(inst);
        if (branch != null) {
            HNode child = m_children.get(branch);
            if (child != null) {
                return child.leafForInstance(inst, this, branch);
            }
            return new LeafNode(null, this, branch);
        }
        return new LeafNode(this, parent, parentBranch);
    }

    @Override
    public void updateNode(Instance inst) {
        // don't update the distribution
    }

    @Override
    public int dumpTree(int depth, int leafCount, StringBuffer buff) {

        for (Map.Entry<String, HNode> e : m_children.entrySet()) {

            HNode child = e.getValue();
            String branch = e.getKey();

            if (child != null) {

                buff.append("\n");

                for (int i = 0; i < depth; i++) {
                    buff.append("|   ");
                }

                buff.append(m_split.conditionForBranch(branch).trim());
                buff.append(": ");
                leafCount = child.dumpTree(depth + 1, leafCount, buff);
            }
        }
        return leafCount;
    }

    @Override
    public int installNodeNums(int nodeNum) {
        nodeNum = super.installNodeNums(nodeNum);

        for (Map.Entry<String, HNode> e : m_children.entrySet()) {

            HNode child = e.getValue();

            if (child != null) {
                nodeNum = child.installNodeNums(nodeNum);
            }
        }

        return nodeNum;
    }

    @Override
    public void graphTree(StringBuffer buff) {
        boolean first = true;
        for (Map.Entry<String, HNode> e : m_children.entrySet()) {

            HNode child = e.getValue();
            String branch = e.getKey();

            if (child != null) {
                String conditionForBranch = m_split.conditionForBranch(branch);
                if (first) {
                    String testAttName = null;

                    if (conditionForBranch.indexOf("<=") < 0) {
                        testAttName = conditionForBranch.substring(0,
                                conditionForBranch.indexOf("=")).trim();
                    } else {
                        testAttName = conditionForBranch.substring(0,
                                conditionForBranch.indexOf("<")).trim();
                    }
                    first = false;
                    buff.append("N" + m_nodeNum + " [label=\"" + testAttName + "\"]\n");
                }

                int startIndex = 0;
                if (conditionForBranch.indexOf("<=") > 0) {
                    startIndex = conditionForBranch.indexOf("<") - 1;
                } else if (conditionForBranch.indexOf("=") > 0) {
                    startIndex = conditionForBranch.indexOf("=") - 1;
                } else {
                    startIndex = conditionForBranch.indexOf(">") - 1;
                }
                conditionForBranch = conditionForBranch.substring(startIndex,
                        conditionForBranch.length()).trim();

                buff.append(
                        "N" + m_nodeNum + "->" + "N" + child.getM_nodeNum() + "[label=\""
                                + conditionForBranch + "\"]\n").append("\n");

            }
        }

        for (Map.Entry<String, HNode> e : m_children.entrySet()) {
            HNode child = e.getValue();

            if (child != null) {
                child.graphTree(buff);
            }
        }
    }

    @Override
    public void printLeafModels(StringBuffer buff) {
        for (Map.Entry<String, HNode> e : m_children.entrySet()) {

            HNode child = e.getValue();

            if (child != null) {
                child.printLeafModels(buff);
            }
        }
    }

}