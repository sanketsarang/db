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

package com.blobcity.db.cluster.connection;

import com.blobcity.db.cluster.nodes.ClusterNodesStore;
import com.blobcity.db.constants.Ports;

import java.io.IOException;
import java.net.Socket;
import javax.annotation.PostConstruct;
import com.blobcity.db.exceptions.OperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class ClusterConnectionListener extends ConnectionEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(ClusterConnectionListener.class);

    @Autowired
    private ClusterNodesStore clusterNodesStore;
    @Autowired
    private ConnectionManager connectionManager;
    @Autowired
    private ConnectionStore connectionStore;

    public ClusterConnectionListener() {
        super(Ports.CLUSTER_PORT);
    }

    @PostConstruct
    private void init() {

        System.out.println("Cluster connection listener will now start");

        /* Starts a thread that listens to incoming TCP connections for inter node connection on cluster */
        start();
    }

    @Override
    protected void processNewClient(Socket socket) {
        //TODO: Start a new ClusterConnection and add it to the ClusterStore

        System.out.println("New Socket connection received");

        String remoteNodeId;

        /**
         * Simply add a connection, this operation does not necessary add the node into the cluster for the node to
         * receive clustered queries. `add-node` operation ect are performed after the connection is added into the
         * pool. Most of the times, this method will be called to simply increase the connection pool size of existing
         * cluster nodes, or in reopening a connection after a network failure.
         */

        try {
            remoteNodeId = connectionManager.newIncomingConnection(socket);
            ClusterConnection clusterConnection = new ClusterConnection(socket, clusterNodesStore.getSelfId(), remoteNodeId);
            connectionStore.addConnection(remoteNodeId, clusterConnection);
            clusterConnection.start(); //start the connection listener. All new messages will be listened to and processed by the ProcessHandler
            System.out.println("Started connection with " + remoteNodeId);
        } catch(OperationException ex) {
            ex.printStackTrace();
            logger.error("Error exchanging connection header with incoming node. The connection will be terminated");
            try {
                socket.close();
            } catch (IOException ex1) {
                logger.error(ex1.getMessage(), ex1);
            }
        }
    }
}
