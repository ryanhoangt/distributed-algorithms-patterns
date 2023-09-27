package com.ryan.gossip;

import java.util.Map;

public class GossipStateMessage {

    private final NodeId fromNode;
    private final Map<NodeId, NodeState> nodeStates;

    public GossipStateMessage(NodeId fromNode, Map<NodeId, NodeState> nodeStates) {
        this.fromNode = fromNode;
        this.nodeStates = nodeStates;
    }
}
