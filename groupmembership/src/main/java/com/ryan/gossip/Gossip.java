package com.ryan.gossip;

import com.ryan.net.InetAddressAndPort;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Gossip {
    private InetAddressAndPort listenAddress;
    private List<InetAddressAndPort> seedNodes;
    private NodeId nodeId;

    private Map<NodeId, NodeState> clusterMetadata = new HashMap<>();

    public Gossip(InetAddressAndPort listenAddress,
                  List<InetAddressAndPort> seedNodes,
                  String nodeId) {
        this.listenAddress = listenAddress;
        this.seedNodes = seedNodes; // TODO: filter this node itself in case it is part of the seed nodes
        this.nodeId = new NodeId(nodeId);

        // TODO: at startup, add metadata about itself that needs to propagated to other nodes

        // TODO: start a socket server
    }
}
