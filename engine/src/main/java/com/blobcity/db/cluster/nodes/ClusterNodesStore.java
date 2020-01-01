/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.cluster.nodes;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.config.DbConfigBean;
import com.blobcity.db.exceptions.OperationException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;

//import com.blobcity.license.License;
import com.blobcity.db.license.LicenseRules;
import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This stores nodes that are members of the cluster.
 *
 * @author sanketsarang
 */
@Component
public class ClusterNodesStore {

    public static String selfId = null;
    private String clusterId =  null;
    private Set<String> clusterNodes = new ConcurrentHashSet<>();
    private Set<String> onlineNodes = new ConcurrentHashSet<>();
    private Map<String, NodeStatus> statusMap = new ConcurrentHashMap<>();
    private static ClusterNodesStore clusterBeanInstance;
    private static final Logger logger = LoggerFactory.getLogger(ClusterNodesStore.class.getName());

    @Autowired
    private ConfigBean configBean;
    @Autowired
    private DbConfigBean dbConfigBean;
    @Autowired
    private BSqlDataManager dataManager;

    @PostConstruct
    private void init() {
        clusterBeanInstance = this;
        clusterBeanInstance.loadClusterNodes();
        System.out.println("SelfId: " + selfId);
    }

    public static ClusterNodesStore getInstance() {
        return clusterBeanInstance;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setClusterId(final String clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterId() {
        return this.clusterId;
    }

    public NodeStatus getNodeStatus(final String nodeId) {
        return statusMap.get(nodeId);
    }

    public void setNodeStatus(final String nodeId, final NodeStatus nodeStatus) {
        statusMap.put(nodeId, nodeStatus);
    }

    /**
     * Call this to initiate connection with a node that was already part of the cluster. Typically called on system
     * boot, to reestablish connectivity with other cluster nodes
     */
    public void connectToNode(final String nodeId, final String ip, final String port) {
        //need to figure this out
    }

    /**
     * Notifies the store of a new node addition to the cluster, and adds the node-id of the newly added node to the
     * cached list of connected nodes. This function is a no-op if the node-id is already cached in the store.
     *
     * @param nodeId the node-id of the node that was added to the cluster
     */
    public void notifyAddNode(final String nodeId) {
        clusterNodes.add(nodeId);
        statusMap.put(nodeId, NodeStatus.UNKNOWN);
    }

    /**
     * Notifies the store of a node removal from the cluster. The function is a no-op if the node-id is not a member of
     * the stores cache.
     *
     * @param nodeId the node-id of the node that was removed from the cluster
     */
    public void notifyRemoveNode(final String nodeId) {
        clusterNodes.remove(nodeId);
        statusMap.remove(nodeId);
    }

    public boolean hasNode(String nodeId) throws OperationException {
        return clusterNodes.contains(nodeId);
    }
    
    /**
     * Gets all the nodes currently part of the cluster
     * @return a {@link Set} of nodes currently part of the cluster
     */
    public Set<String> getAllNodes() {
        return Collections.unmodifiableSet(clusterNodes);
    }

    /**
     * Gets a collection of currently least loaded nodes. The number of nodes returned are expected to satisfy the
     * replication factor specified, but can be lesser than the replication factor if the required number of nodes
     * are not available.
     * @param replicationFactor the replication factor. 0 means no replication, and -1 means full replication for
     *                          mirrored type collections
     * @return a {@link Set} of node-id's belonging to nodes that are currently least loaded
     */
    public Set<String> getLeastLoadedNodes(int replicationFactor) {
        if(replicationFactor == -1 || clusterNodes.size() == 1) {
            return Collections.unmodifiableSet(clusterNodes);
        }

        //TODO: For no replication, improve to send data to least loaded nodes than sending to self node
        if(replicationFactor == 0) {
            return new HashSet<>(Arrays.asList(getSelfId()));
        }

        //TODO: implement the load factor based return of nodes
        throw new UnsupportedOperationException("not supported yet.");
    }

//    private void loadClusterNodes() {
//        List<JSONObject> nodes = Collections.emptyList();
//        try {
//            nodes = dataManager.selectAll(".systemdb", "nodes");
//        } catch(OperationException ex) {
//            logger.error("Unable to read internal nodes table. Defaulting to single node operation. If this node was part" +
//                    "of a cluster, it must be shutdown immediately to prevent data corruption");
//        }
//
//        if(nodes.isEmpty()) {
//            logger.warn("Could not find cluster configuration. Defaulting to single node mode. If this node is expected to "
//                    + "be part of a cluster, you must shut down the node immediately to prevent data corruption and "
//                    + "list the cluster configuration inside config.json property file before booting.");
//        }
//
//        nodes.forEach(node -> {
//            System.out.println("Found node: " + node.toString());
//        });
//
//        try {
//            dbConfigBean.loadAllConfigs();
//            if (LicenseRules.SELF_NODE_ID.isEmpty())
//                dbConfigBean.setConfig("SELF_NODE_ID", UUID.randomUUID().toString());
//        } catch(OperationException ex) {
//            logger.error("Error assigning SELF_NODE_ID  to new node. Clustering will fail", ex);
//            LicenseRules.SELF_NODE_ID = "default";
//        }
//
//        String selfNodeId = LicenseRules.SELF_NODE_ID;
//        if (selfNodeId == null) {
//            logger.warn("Self node id is not configured. Cluster may not function correctly");
//        } else {
//            clusterNodes.add(selfNodeId);
//            this.selfId = selfNodeId;
//        }
//
//        System.out.println("SELF_NODE_ID=" + selfNodeId);
//    }

    private void loadClusterNodes() {
        Object obj = configBean.getProperty(ConfigProperties.CLUSTER_NODES);
        if (obj == null) {
            logger.warn("Could not find cluster configuration. Defaulting to single node mode. If this node is expected to "
                    + "be part of a cluster, you must shut down the node immediately to prevent data corruption and "
                    + "list the cluster configuration inside config.json property file before booting.");
        } else {
            JSONArray jsonArray = (JSONArray) configBean.getProperty(ConfigProperties.CLUSTER_NODES);
            clusterNodes.clear();
            for (int i = 0; i < jsonArray.length(); i++) {
                clusterNodes.add(jsonArray.getString(i));
            }
        }

//        String selfNodeId = configBean.getStringProperty(ConfigProperties.NODE_ID);
//        String selfNodeId = License.getNodeId();

        /* Assign a new node id if one is not already saved inside the DbConfig table */

        try {
            dbConfigBean.loadAllConfigs();
            if (LicenseRules.SELF_NODE_ID.isEmpty())
                dbConfigBean.setConfig("SELF_NODE_ID", UUID.randomUUID().toString());
        } catch(OperationException ex) {
            logger.error("Error assigning SELF_NODE_ID to new node. Clustering will fail", ex);
            LicenseRules.SELF_NODE_ID = "default";
        }



        String selfNodeId = LicenseRules.SELF_NODE_ID;
        if (selfNodeId == null) {
            logger.warn("Self node id is not configured. Cluster may not function correctly");
        } else {
            clusterNodes.add(selfNodeId);
            this.selfId = selfNodeId;
        }

        System.out.println("SELF_NODE_ID=" + selfNodeId);
    }
}
