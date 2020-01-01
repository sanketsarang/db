package com.blobcity.db.cluster.nodes;

public enum NodeStatus {
    CONNECTED,
    NEW_NODE,
    DOWN,
    SYNCING,
    READY_TO_CONNECT,
    UNKNOWN //typically assigned to nodes that just got connected. It could be a new node, or an existing node, but the same is yet to be found out
}
