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
package org.wso2.siddhi.classifiers.trees.ht.nodes;

import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.SplitNode;
import org.wso2.siddhi.classifiers.trees.ht.nodes.HNode;

public class LeafNode extends HNode {
    /**
     * For serialization
     */
    private static final long serialVersionUID = -3359429731894384404L;

    /** The actual node for this leaf */
    public HNode m_theNode;

    /** Parent split node */
    public SplitNode m_parentNode;

    /** Parent branch leading to this node */
    public String m_parentBranch;

    /**
     * Construct an empty leaf node
     */
    public LeafNode() {
    }

    /**
     * Construct a leaf node with the given actual node, parent and parent branch
     *
     * @param node the actual node at this leaf
     * @param parentNode the parent split node
     * @param parentBranch the branch leading to this node
     */
    public LeafNode(HNode node, SplitNode parentNode, String parentBranch) {
        m_theNode = node;
        m_parentNode = parentNode;
        m_parentBranch = parentBranch;
    }

    @Override
    public void updateNode(Instance inst) throws Exception {
        if (m_theNode != null) {
            m_theNode.updateDistribution(inst);
        } else {
            super.updateDistribution(inst);
        }
    }
}
