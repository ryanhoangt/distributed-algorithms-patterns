package com.ryan.gossip;

import com.ryan.common.JsonSerDes;
import com.ryan.common.Request;
import com.ryan.net.InetAddressAndPort;
import com.ryan.net.SocketClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Gossip {
    private static final Logger logger = LogManager.getLogger(Gossip.class);
    private final Random rand = new Random();
    
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
        if (nodeList.isEmpty()) return;

        for (int i = 0; i < gossipFanout; i++) {
            InetAddressAndPort nodeAddr = pickRandomNode(nodeList);
            sendGossipTo(nodeAddr);
        }
    }

    private void sendGossipTo(InetAddressAndPort nodeAddr) {
        try {
            logger.info("Sending gossip state to: " + nodeAddr);
            SocketClient socketClient = new SocketClient(nodeAddr);
            var gossipStateMessage = new GossipStateMessage(this.nodeId, this.clusterMetadata);

            // TODO: serialize the message to bytes in Request object
            Request request = createPushGossipStateRequest(gossipStateMessage);

            // TODO: send the message through the SocketClient instance & wait for response

            // TODO: deserialize the response

            // TODO: merge the response's state map into the local state map

        } catch (IOException ex) {
            logger.error("IO error while sending messages to: " + nodeAddr, ex);
        }
    }

    private Request createPushGossipStateRequest(GossipStateMessage gossipStateMessage) {
        return new Request(RequestType.PushGossipState.getId(), JsonSerDes.serialize(gossipStateMessage));
    }

    private InetAddressAndPort pickRandomNode(List<InetAddressAndPort> nodeList) {
        int nodeIdx = rand.nextInt(nodeList.size());
        return nodeList.get(nodeIdx);
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
