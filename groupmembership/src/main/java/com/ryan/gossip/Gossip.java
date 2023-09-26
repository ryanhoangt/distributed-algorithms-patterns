package com.ryan.gossip;

import com.ryan.net.InetAddressAndPort;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Gossip {
    private InetAddressAndPort listenAddress;
    private List<InetAddressAndPort> seedNodes;
    private NodeId nodeId;

    /**
     * Local metadata maintained by the current node about each node in the cluster.
     * This will be passed to other nodes to propagate states.
     */
    private Map<NodeId, NodeState> clusterMetadata = new HashMap<>();

    private ScheduledThreadPoolExecutor gossipExecutor = new ScheduledThreadPoolExecutor(1);
    private long gossipIntervalMs = 1000;
    private int gossipFanout = 2;
    private ScheduledFuture<?> taskFuture;

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

    public void start() {
        // TODO: start the initialized socket server

        taskFuture = gossipExecutor.scheduleAtFixedRate(this::doGossip,
                gossipIntervalMs, gossipIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void doGossip() {
        List<InetAddressAndPort> knownClusterNodes = liveNodes();
        if (knownClusterNodes.isEmpty())
            sendGossip(seedNodes, gossipFanout);
        else // no nodes are known yet, send to introducers
            sendGossip(knownClusterNodes, gossipFanout);
    }

    private void sendGossip(List<InetAddressAndPort> nodeList, int gossipFanout) {
        // TODO: implement gossip communication using either TCP or UDP
        
    }

    private List<InetAddressAndPort> liveNodes() {
        Set<InetAddressAndPort> nodes = clusterMetadata.values().stream()
                .map(nodeState -> InetAddressAndPort.parse(nodeState.get(GossipKeys.ADDRESS).getValue()))
                .collect(Collectors.toSet());
        return removeSelfAddress(nodes);
    }

    private void addLocalState(String key, String value) {
        NodeState nodeState = clusterMetadata.get(this.nodeId);
        if (nodeState == null) {
            nodeState = new NodeState();
            clusterMetadata.put(nodeId, nodeState);
        }
        nodeState.add(key, new VersionedValue(value, 1)); // FIXME: fixed version number for now
    }

    private List<InetAddressAndPort> removeSelfAddress(Collection<InetAddressAndPort> nodes) {
        return nodes.stream().filter(aap -> !aap.equals(listenAddress))
                .collect(Collectors.toList());
    }
}
