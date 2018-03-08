/*
 * Copyright (c) 2013-2017 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.horne.cdbg.sp;

import com.graphaware.common.log.LoggerFactory;

import com.graphaware.common.util.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.event.improved.api.LazyTransactionData;


import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import static com.graphaware.common.util.PropertyContainerUtils.nodeToString;
import static com.graphaware.common.util.PropertyContainerUtils.relationshipToString;
import static com.graphaware.common.util.PropertyContainerUtils.propertiesToString;

/**
 * Example of a Neo4j {@link org.neo4j.graphdb.event.TransactionEventHandler} that uses GraphAware {@link ImprovedTransactionData}
 * to do its job, which is counting the total strength of all friendships in the database and writing that to a special
 * node created for that purpose.
 */
public class ChangeLogger extends TransactionEventHandler.Adapter<Void> {

    private static final Log LOG = LoggerFactory.getLogger(ChangeLogger.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(TransactionData data) throws Exception {
        logChanges(new LazyTransactionData(data));

        return null;
    }

    private void logChanges(ImprovedTransactionData improvedData) {
        Set<String> changes = new HashSet<>();

        for (Node createdNode : improvedData.getAllCreatedNodes()) {
            changes.add("Created node " + nodeToString(createdNode));
        }

        for (Node deletedNode : improvedData.getAllDeletedNodes()) {
            changes.add("Deleted node " + nodeToString(deletedNode));
        }

        for (Change<Node> changedNode : improvedData.getAllChangedNodes()) {
        	Node current = changedNode.getCurrent();
        	Node prev = changedNode.getPrevious();
            changes.add("Changed node " + nodeToString(changedNode.getPrevious()) + " to " + nodeToString(current));
            Map<String, Change<Object>> mm = improvedData.changedProperties(changedNode.getCurrent());
            
            for (Map.Entry<String, Change<Object>> entry : mm.entrySet())
            {
                // System.out.println(entry.getKey() + "/" + entry.getValue());
            	// String thisChange = "Property changed: " + entry.getKey() + "/" + entry.getValue().toString();
            	String key = entry.getKey();
            	String thisChange = "Property changed: " + key + "/" + "FROM:" + prev.getProperty(key) + " TO: " + current.getProperty(key);
            	
            	changes.add(thisChange);
            	
            }
            
        }

        for (Relationship createdRelationship : improvedData.getAllCreatedRelationships()) {
            changes.add("Created relationship " + relationshipToString(createdRelationship));
        }

        for (Relationship deletedRelationship : improvedData.getAllDeletedRelationships()) {
            changes.add("Deleted relationship " + relationshipToString(deletedRelationship));
        }

        for (Change<Relationship> changedRelationship : improvedData.getAllChangedRelationships()) {
            changes.add("Changed relationship " + relationshipToString(changedRelationship.getPrevious()) + " to " + relationshipToString(changedRelationship.getCurrent()));
        }

        for (String change : changes) {
            LOG.info(change);
        }
    }
}
