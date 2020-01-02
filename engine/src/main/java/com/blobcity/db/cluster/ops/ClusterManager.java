package com.blobcity.db.cluster.ops;

import com.blobcity.db.cluster.nodes.ClusterNodesStore;
import com.blobcity.db.config.DbConfigBean;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ClusterManager {

    @Autowired
    private DbConfigBean dbConfigBean;
    @Autowired
    private ClusterNodesStore clusterNodesStore;

    /**
     * Should be used to convert a single node into a clustered operating mode. This is used when forming a new cluster
     * and used on the first node in the cluster. All new nodes must add into this node.
     */
    public String createCluster() throws OperationException {
        if(clusterNodesStore.getClusterId() != null) {
            throw new OperationException(ErrorCode.ALREADY_A_CLUSTER);
        }
        final String clusterId = UUID.randomUUID().toString();
        dbConfigBean.setConfig("OPERATING_MODE", "cluster");
        dbConfigBean.setConfig("CLUSTER_ID", clusterId);
        clusterNodesStore.setClusterId(clusterId);
        return clusterId;
    }

    /**
     * Will convert single node to operate in a non clustered manner. However if this operation is run on an existing
     * cluster, all nodes will be made to operate in a standalone mode
     */
    public void dropCluster()  {
        //TODO: Implement this
    }
}