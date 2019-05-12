package org.ethereum.net.rlpx.discover;

import com.google.common.collect.RangeMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
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
    private final static int WRITE_ITERS = 100;

    private static CrawlerGraph instance = null; //singleton instance because I'm lazy

    private NodeManager manager;
    private Set<Node> allNodes;
    private Set<Node> toAdd;
    private MutableGraph<Node> graph;
    private int iters;

    private static RangeMap<Long, Triple<String, Double, Double>> geo;

    static final org.slf4j.Logger logger = LoggerFactory.getLogger("discover");
    private static final Object lock = new Object();

    public CrawlerGraph(NodeManager manager) {
        this.manager = manager;
        this.allNodes = new HashSet<>();
        this.toAdd = new HashSet<>();
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
        int i = 0;
        while(true) {
            for (Node node : allNodes) {
                manager.getNodeHandler(manager.homeNode).sendFindNode(node.getId());

                try {
                    Thread.sleep(5); //take a breather to try to avoid overloading the network
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            synchronized (lock) {
                allNodes.addAll(toAdd);
                toAdd = new HashSet<>();
            }
        }
    }

    private void discover() {
        logger.info("Sending discovery");
        Set<Node> toDiscover = new HashSet<>();
        synchronized (lock) {
            toDiscover.addAll(allNodes);
            logger.info("Discovered: " + allNodes.size());
        }
        for(NodeEntry entry : manager.getTable().getAllNodes()) {
            toDiscover.add(entry.getNode());
        }
        for (Node node : toDiscover) {
            manager.getNodeHandler(manager.homeNode).sendFindNode(node.getId());
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
			allNodes.add(node);
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
        Node target = getNodeWithId(((NeighborsMessage)evt.getMessage()).getNodeId());
        if(target == null) {
            logger.warn("NULL NODE FOUND");
            return;
        }
        Collection<Node> nodes = ((NeighborsMessage)evt.getMessage()).getNodes();

        synchronized (lock) {
            this.toAdd.addAll(nodes);
        }

        for(Node neighbour : nodes) {
            if(!graph.nodes().contains(neighbour)) {
                graph.addNode(neighbour);
            }
            graph.putEdge(target, neighbour);
        }

        logger.info("" + graph.nodes().size());

        if(iters % WRITE_ITERS == 0) {
            logger.info("WRITING GRAPH TO FILE");
            try {
                toFile();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        iters++;
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

    public void recursiveCrawl() {
        logger.info("CRAWLING");

        Queue<Node> toExplore = new LinkedList<>();
        Set<Node> alreadyExplored = new HashSet<>();

        toExplore.add(manager.getTable().getNode());
        alreadyExplored.add(manager.getTable().getNode());

        while(!toExplore.isEmpty()) {
            Node next = toExplore.poll();
            if(!graph.nodes().contains(next)) {
                graph.addNode(next);
            }
            Collection<Node> neighbours = manager.getTable().getClosestNodes(next.getId());
            for(Node neighbour : neighbours) {
                if(!graph.nodes().contains(neighbour)) {
                    graph.addNode(neighbour);
                }

                if(!alreadyExplored.contains(neighbour)) {
                    toExplore.add(neighbour);
                    graph.putEdge(neighbour, next);
                }
            }
            alreadyExplored.add(next);
        }

        try {
            toFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*public void recursiveCrawl() throws IOException {
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

    }*/

    private void toFile() throws IOException {
        /*Set<LocationOutput> locOut = new HashSet<>();
        Set<NodeOutput> nodeOut = new HashSet<>();
        Set<LinkOutput> linkOut = new HashSet<>();*/
        Gson gson = new Gson();



        /*for(Node node : graph.nodes()) {
            Triple<String, Double, Double> loc = getGeo(node.getHost());
            if(loc != null) {
                locOut.add(new LocationOutput(loc.getLeft(), loc.getMiddle(), loc.getRight()));
				if(Arrays.equals(node.getId(), manager.getTable().getNode().getId())) {
					nodeOut.add(new NodeOutput(node.getHost(), loc.getLeft(), true));
				} else {
					nodeOut.add(new NodeOutput(node.getHost(), loc.getLeft()));
				}
            }
        }
        for(EndpointPair<Node> pair : graph.edges()) {
            linkOut.add(new LinkOutput(pair.nodeU().getHost(), pair.nodeV().getHost()));
        }*/

        Output out = new Output();
        OutputWithLocation locs = new OutputWithLocation();


        try (BufferedWriter nodeFile = new BufferedWriter(new FileWriter(NODE_FILE))) {
            nodeFile.write(gson.toJson(out));
        }
        try (BufferedWriter locFile = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            locFile.write(gson.toJson(locs));
        }
        /*try (BufferedWriter locFile = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            locFile.write(gson.toJson(new ArrayList<>(locOut)));
        }
        try (BufferedWriter linkFile = new BufferedWriter(new FileWriter(LINKS_FILE))) {
            linkFile.write(gson.toJson(new ArrayList<>(linkOut)));
        }*/

        logger.info("NODE COUNT: " + allNodes.size());
        logger.info("GRAPH: " + graph.nodes().size());
        logger.info("LINKS: " + graph.edges().size());
        //logger.info("OUTPUT GRAPH: " + nodeOut.size());
        //logger.info("OUTPUT LINK: " + linkOut.size());
    }

    private class Output {
        private List<NodeOutput> nodes;
        private List<LinkOutput> links;

        private Output() throws IOException {
            Set<NodeOutput> nodes = new HashSet<>();
            Set<LinkOutput> links = new HashSet<>();
            Set<String> ipaddrs = new HashSet<>();

            nodes.add(new NodeOutput(manager.getTable().getNode().getHost(), 1));
            for(Node node : Graphs.reachableNodes(graph, manager.getTable().getNode())) {
                //Triple<String, Double, Double> loc = getGeo(node.getHost());
                //if(loc != null) {
                    //locOut.add(new LocationOutput(loc.getLeft(), loc.getMiddle(), loc.getRight()));
                nodes.add(new NodeOutput(node.getHost(), 2));
                ipaddrs.add(node.getHost());
            }

            for(EndpointPair<Node> pair : graph.edges()) {
                if(ipaddrs.contains(pair.nodeU().getHost()) && ipaddrs.contains(pair.nodeV().getHost())) {
                    links.add(new LinkOutput(pair.nodeU().getHost(), pair.nodeV().getHost()));
                }
            }

            logger.info("WRITING " + nodes.size() + " NODES TO FILE");
            this.nodes = new ArrayList<>(nodes);
            this.links = new ArrayList<>(links);
        }
    }

    private class OutputWithLocation {
        private List<LocationOutput> nodes;
        private List<LinkOutput> links;

        private String addLocation(Map<String, LocationOutput> nodes, Node node) {
            Triple<String, Double, Double> geoLoc = getGeo(node.getHost());
            if(geoLoc == null) {
                return null;
            }
            if (!nodes.containsKey(geoLoc.getLeft())) {
                nodes.put(geoLoc.getLeft(), new LocationOutput(geoLoc.getLeft(), geoLoc.getMiddle(), geoLoc.getRight()));
            }
            nodes.get(geoLoc.getLeft()).addNode();
            return geoLoc.getLeft();
        }

        private OutputWithLocation() throws IOException {
            Map<String, LocationOutput> nodes = new HashMap<>();
            Map<String, String> ipToLoc = new HashMap<>();
            Set<LinkOutput> links = new HashSet<>();

            for (Node node : Graphs.reachableNodes(graph, manager.getTable().getNode())) {
                String loc = addLocation(nodes, node);
                if(loc != null) {
                    ipToLoc.put(node.getHost(), loc);
                }
            }
            String loc = addLocation(nodes, manager.getTable().getNode());
            ipToLoc.put(manager.getTable().getNode().getHost(), loc);

            for(EndpointPair<Node> pair : graph.edges()) {
                if(ipToLoc.containsKey(pair.nodeU().getHost()) && ipToLoc.containsValue(pair.nodeV().getHost())) {
                    links.add(new LinkOutput(ipToLoc.get(pair.nodeU().getHost()),
                            ipToLoc.get(pair.nodeV().getHost())));
                }
            }

            logger.info("WRITING " + nodes.size() + " LOCATIONS TO FILE");
            this.nodes = new ArrayList<>(nodes.values());
            this.links = new ArrayList<>(links);
        }
    }

    private class NodeOutput {
        private String id;
        private int group;

        public NodeOutput(String id, int group) {
            this.id = id;
            this.group = group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeOutput that = (NodeOutput) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private class LocationOutput {
        private String id;
        private String name;
        private double latitude;
        private double longitude;
        private int group;
        private int nodes;

        public LocationOutput(String id, double latitude, double longitude) {
            this.id = id;
            this.name = "";
            this.latitude = latitude;
            this.longitude = longitude;
            this.group = 2;
            this.nodes = 0;
        }

        private void setGroup(int group) {
            this.group = group;
        }

        private void addNode() {
            nodes += 1;
            this.name = id + " (" + nodes + " here)";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LocationOutput that = (LocationOutput) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private class LinkOutput {
        private String source;
        private String target;

        public LinkOutput(String source, String target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LinkOutput that = (LinkOutput) o;
            return Objects.equals(source, that.source) &&
                    Objects.equals(target, that.target);
        }

        @Override
        public int hashCode() {
            return source.hashCode() + target.hashCode();
        }
    }

    /*private class LocationOutput {
        private String name;
        private double latitude;
        private double longitude;

        public LocationOutput(String name, double latitude, double longitude) {
            this.name = name;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }*/

    /*private class NodeOutput {
        private String ip;
        private String location;
		private boolean isRoot;

        public NodeOutput(String ip, String location) {
            this(ip, location, false);
        }
		
		public NodeOutput(String ip, String location, boolean isRoot) {
			this.ip = ip;
            this.location = location;
			this.isRoot = isRoot;
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
    }*/
}
