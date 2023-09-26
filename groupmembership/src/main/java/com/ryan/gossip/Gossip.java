package com.ryan.gossip;

import com.ryan.net.InetAddressAndPort;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Gossip {
    private InetAddressAndPort listenAddress;
    private List<InetAddressAndPort> seedNodes;
    private NodeId nodeId;

    private Map<NodeId, NodeState> clusterMetadata = new HashMap<>();

    /**
     * Initialize the gossip instance.
     * @param listenAddress the IP address and port the node listens on
     * @param seedNodes list of introducers/seed nodes
     * @param nodeId the id of the node
     */
    public Gossip(InetAddressAndPort listenAddress,
                  List<InetAddressAndPort> seedNodes,
                  String nodeId) {
        this.listenAddress = listenAddress;
        this.seedNodes = removeSelfAddress(seedNodes);
        this.nodeId = new NodeId(nodeId);

        addLocalState(GossipKeys.ADDRESS, listenAddress.toString());

        // TODO: initialize a socket server
    }

    private void addLocalState(String key, String value) {
        NodeState nodeState = clusterMetadata.get(this.nodeId);
        if (nodeState == null) {
            nodeState = new NodeState();
            clusterMetadata.put(nodeId, nodeState);
        }
        nodeState.add(key, new VersionedValue(value, 1)); // FIXME: fixed version number for now
    }

    private List<InetAddressAndPort> removeSelfAddress(List<InetAddressAndPort> nodes) {
        return nodes.stream().filter(aap -> !aap.equals(listenAddress))
                .collect(Collectors.toList());
    }
}
