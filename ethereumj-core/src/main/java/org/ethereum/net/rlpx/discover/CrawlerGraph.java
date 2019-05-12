package org.ethereum.net.rlpx.discover;

import com.google.common.collect.RangeMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.ethereum.net.rlpx.NeighborsMessage;
import org.ethereum.net.rlpx.Node;
import org.ethereum.net.rlpx.discover.table.NodeEntry;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class CrawlerGraph extends Thread {
    private final static String NODE_FILE = "files/out/nodes.json";
    private final static String LINKS_FILE = "files/out/links.json";
    private final static String LOCATION_FILE = "files/out/locations.json";
    private final static int WRITE_ITERS = 600;

    private static CrawlerGraph instance = null; //singleton instance because I'm lazy

    private NodeManager manager;
    private Set<Node> allNodes;
    private MutableGraph<Node> graph;
    private int iters;

    private static RangeMap<Long, Triple<String, Double, Double>> geo;

    static final org.slf4j.Logger logger = LoggerFactory.getLogger("discover");

    public CrawlerGraph(NodeManager manager) {
        this.manager = manager;
        this.allNodes = new HashSet<>();
        this.graph = GraphBuilder.undirected().allowsSelfLoops(false).build();

        this.allNodes.add(manager.getTable().getNode());
        this.graph.addNode(manager.getTable().getNode());

        iters = 1;
    }

    public static void readDb() {
        logger.info("Starting db retrieval (this will take a few minutes)");
        geo = Geolocator.getDatabase();
        logger.info("Finished db retrieval");
    }

    public static void setup(NodeManager manager) {
        instance = new CrawlerGraph(manager);
    }

    /**
     * Get the current singleton instance
     *
     * @return the current singleton instance
     */
    public static CrawlerGraph get() {
        return instance;
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            discover();
        }
    }

    private void discover() {
        Set<Node> toDiscover = new HashSet<>(allNodes);
        for(NodeEntry entry : manager.getTable().getAllNodes()) {
            toDiscover.add(entry.getNode());
        }
        for (Node node : toDiscover) {
            manager.getNodeHandler(node).sendFindNode(node.getId());
        }
    }

    private Node getNodeWithId(byte[] id) {
        for(NodeEntry entry : manager.getTable().getAllNodes()) {
            if(Arrays.equals(entry.getNode().getId(), id)) {
                return entry.getNode();
            }
        }
        return null;
    }

    private void updateClosest() {
        for(Node node : manager.getTable().getClosestNodes(manager.getTable().getNode().getId())) {
            this.graph.putEdge(manager.getTable().getNode(), node);
        }
    }

    private void removeEdges(Node node) {
        Set<EndpointPair<Node>> toRemove = new HashSet<>();
        for(EndpointPair<Node> pair : graph.edges()) {
            if(pair.nodeU().equals(node) || pair.nodeV().equals(node)) {
                toRemove.add(pair);
            }
        }
        for(EndpointPair<Node> pair : toRemove) {
            graph.removeEdge(pair.nodeU(), pair.nodeV());
        }
    }

    public void addNodes(DiscoveryEvent evt) {
        updateClosest();
        Node target = getNodeWithId(evt.getMessage().getNodeId());
        if(target == null) {
            return; //unknown node
        }
        if(!allNodes.contains(target)) {
            allNodes.add(target);
            graph.addNode(target);
        } else {
            removeEdges(target);
        }
        for(Node neighbour : ((NeighborsMessage)evt.getMessage()).getNodes()) {
            if(!allNodes.contains(neighbour)) {
                allNodes.add(neighbour);
                graph.addNode(neighbour);
            }
            graph.putEdge(target, neighbour);
        }
        if(iters++ % WRITE_ITERS == 0) {
            try {
                toFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Triple<String, Double, Double> getGeo(String ipaddr) {
        try {
            InetAddress i = Inet4Address.getByName(ipaddr);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
            buffer.put(new byte[] { 0,0,0,0 });
            buffer.put(i.getAddress());
            buffer.position(0);
            long converted = buffer.getLong();
            return geo.get(converted);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void recursiveCrawl() throws IOException {
        Stack<Node> toExplore = new Stack<>();
        Set<Node> alreadyExplored = new HashSet<>();
        Set<Pair<Node, Node>> links = new HashSet<>();

        toExplore.push(manager.getTable().getNode());
        alreadyExplored.add(manager.getTable().getNode());

        while(!toExplore.isEmpty()) {
            Node next = toExplore.pop();
            Collection<Node> neighbours = manager.getTable().getClosestNodes(next.getId());
            for(Node neighbour : neighbours) {
                if(!links.contains(new ImmutablePair<>(next, neighbour))) {
                    links.add(new ImmutablePair<>(neighbour, next));
                }
                if(!alreadyExplored.contains(neighbour)) {
                    toExplore.push(neighbour);
                    alreadyExplored.add(neighbour);
                }
            }
        }

        Set<LocationOutput> locOut = new HashSet<>();
        Set<NodeOutput> nodeOut = new HashSet<>();
        Set<LinkOutput> linkOut = new HashSet<>();
        Gson gson = new Gson();

        for(Node node : alreadyExplored) {
            Triple<String, Double, Double> loc = getGeo(node.getHost());
            if(loc != null) {
                locOut.add(new LocationOutput(loc.getLeft(), loc.getMiddle(), loc.getRight()));
                nodeOut.add(new NodeOutput(node.getHost(), loc.getLeft()));
            }
        }
        for(Pair<Node, Node> pair : links) {
            linkOut.add(new LinkOutput(pair.getLeft().getHost(), pair.getRight().getHost()));
        }

        try (BufferedWriter nodeFile = new BufferedWriter(new FileWriter(NODE_FILE))) {
            nodeFile.write(gson.toJson(new ArrayList<>(nodeOut)));
        }
        try (BufferedWriter locFile = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            locFile.write(gson.toJson(new ArrayList<>(locOut)));
        }
        try (BufferedWriter linkFile = new BufferedWriter(new FileWriter(LINKS_FILE))) {
            linkFile.write(gson.toJson(new ArrayList<>(linkOut)));
        }

    }

    private void toFile() throws IOException {
        Set<LocationOutput> locOut = new HashSet<>();
        Set<NodeOutput> nodeOut = new HashSet<>();
        Set<LinkOutput> linkOut = new HashSet<>();
        Gson gson = new Gson();

        for(Node node : graph.nodes()) {
            Triple<String, Double, Double> loc = getGeo(node.getHost());
            if(loc != null) {
                locOut.add(new LocationOutput(loc.getLeft(), loc.getMiddle(), loc.getRight()));
                nodeOut.add(new NodeOutput(node.getHost(), loc.getLeft()));
            }
        }
        for(EndpointPair<Node> pair : graph.edges()) {
            linkOut.add(new LinkOutput(pair.nodeU().getHost(), pair.nodeV().getHost()));
        }

        try (BufferedWriter nodeFile = new BufferedWriter(new FileWriter(NODE_FILE))) {
            nodeFile.write(gson.toJson(new ArrayList<>(nodeOut)));
        }
        try (BufferedWriter locFile = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            locFile.write(gson.toJson(new ArrayList<>(locOut)));
        }
        try (BufferedWriter linkFile = new BufferedWriter(new FileWriter(LINKS_FILE))) {
            linkFile.write(gson.toJson(new ArrayList<>(linkOut)));
        }

        logger.info("NODE COUNT: " + allNodes.size());
        logger.info("GRAPH: " + graph.nodes().size());
        logger.info("LINKS: " + graph.edges().size());
        logger.info("OUTPUT GRAPH: " + nodeOut.size());
        logger.info("OUTPUT LINK: " + linkOut.size());
    }

    private class LocationOutput {
        private String name;
        private double latitude;
        private double longitude;

        public LocationOutput(String name, double latitude, double longitude) {
            this.name = name;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    private class NodeOutput {
        private String ip;
        private String location;

        public NodeOutput(String ip, String location) {
            this.ip = ip;
            this.location = location;
        }
    }

    private class LinkOutput {
        private String srcIp;
        private String dstIp;

        public LinkOutput(String srcIp, String dstIp) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LinkOutput that = (LinkOutput) o;
            return Objects.equals(srcIp, that.srcIp) &&
                    Objects.equals(dstIp, that.dstIp);
        }

        @Override
        public int hashCode() {
            return srcIp.hashCode() + dstIp.hashCode();
        }
    }
}
