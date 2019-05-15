package org.ethereum.net.rlpx.discover;

import com.google.common.collect.RangeMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Triple;
import org.ethereum.net.rlpx.NeighborsMessage;
import org.ethereum.net.rlpx.Node;
import org.ethereum.net.rlpx.discover.table.NodeEntry;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Crawler singleton class
 */
public class CrawlerGraph extends Thread {
    private final static String NODE_FILE = "files/out/nodes.json";
    private final static String LOCATION_FILE = "files/out/locations.json";
    private final static int WRITE_ITERS = 100; //larger number = write less often, smaller number = write regularly

    private NodeManager manager; //used to do all the important networky things
    private Set<Node> allNodes; //a set containing all the nodes in our graph
    private Set<Node> toAdd; //a set containing the most recent nodes we have discovered
    private MutableGraph<Node> graph; //the network graph
    private int iters; //how many disovery messages we have processed
    private static RangeMap<Long, Triple<String, Double, Double>> geo; //mapping of IP addresses to locations

    private final static boolean DB_ENABLED = true; //change to true to do DB stuff
    private final static String PASSWORD_FILE = "files/password.txt";
    private Connection conn; //database connection
    private String dbPassword; //password for the database

    private static CrawlerGraph instance = null; //singleton instance because I'm lazy
    static final org.slf4j.Logger logger = LoggerFactory.getLogger("discover");
    private static final Object lock = new Object(); //thread safety is important

    /**
     * Constructs a new node crawler with the given node manager
     *
     * @param manager the node manager to do all the networky things
     */
    public CrawlerGraph(NodeManager manager) {
        this.manager = manager;
        this.allNodes = new HashSet<>();
        this.toAdd = new HashSet<>();
        this.graph = GraphBuilder.undirected().allowsSelfLoops(false).build();

        this.allNodes.add(manager.getTable().getNode());
        this.graph.addNode(manager.getTable().getNode());

        iters = 1;
    }

    private void connectToDb() {
        /*try (BufferedReader reader = new BufferedReader(new FileReader(PASSWORD_FILE))) {
            dbPassword = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://happymappy.braewebb.com:5432/happy",
                    "postgres", "bVawU6etXvWwQgsR");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            conn = null;
        }
    }

    /**
     * Load the geolocation database
     * <p>
     * This usually takes a minute or two to run
     */
    public static void readGeoData() {
        logger.info("Starting db retrieval (this will take a few minutes)");
        geo = Geolocator.getDatabase();
        logger.info("Finished db retrieval");
    }

    /**
     * Initialise the singleton instance
     *
     * @param manager the node manager that the instance will uses
     */
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

    /**
     * Sends out periodic discovery messages
     */
    @Override
    public void run() {
        int i = 0;
        while (true) {
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

    /**
     * Get the node in the network with the given ID
     *
     * @param id the ID of the node to retrieve
     * @return the node with that ID, or null if no node exists
     */
    private Node getNodeWithId(byte[] id) {
        for (NodeEntry entry : manager.getTable().getAllNodes()) {
            if (Arrays.equals(entry.getNode().getId(), id)) {
                return entry.getNode();
            }
        }
        return null;
    }

    /**
     * Remove all edges from a node
     * <p>
     * Currently unused as it tends to break the crawler
     *
     * @param node the node to remove all edges from
     */
    @Deprecated
    private void removeEdges(Node node) {
        Set<EndpointPair<Node>> toRemove = new HashSet<>();
        for (EndpointPair<Node> pair : graph.edges()) {
            if (pair.nodeU().equals(node) || pair.nodeV().equals(node)) {
                toRemove.add(pair);
            }
        }
        for (EndpointPair<Node> pair : toRemove) {
            graph.removeEdge(pair.nodeU(), pair.nodeV());
        }
    }

    /**
     * Handle a neighbours message and add those neighbours to the graph
     *
     * @param evt the neighbours message to handle
     */
    public void addNodes(DiscoveryEvent evt) {
        for (Node neighbour : manager.getTable().getClosestNodes(manager.homeNode.getId())) {
            if (!graph.nodes().contains(neighbour)) {
                graph.addNode(neighbour);
                graph.putEdge(manager.homeNode, neighbour);
                synchronized (lock) {
                    toAdd.add(neighbour);
                }
            }
        }

        Node target = getNodeWithId(evt.getMessage().getNodeId());
        if (target == null) {
            return;
        }
        Collection<Node> nodes = ((NeighborsMessage) evt.getMessage()).getNodes();

        for (Node neighbour : nodes) {
            if (!graph.nodes().contains(neighbour)) {
                graph.addNode(neighbour);
            }
            try {
                graph.putEdge(target, neighbour);
            } catch (IllegalArgumentException ex) {} //thrown when self loop occurs
        }

        synchronized (lock) {
            this.toAdd.addAll(nodes);
        }

        //logger.info("" + graph.nodes().size()); //uncomment this to constantly view the number of nodes

        if (iters % WRITE_ITERS == 0) {
            logger.info("WRITING GRAPH TO FILE");
            if(DB_ENABLED) {
                toDb();
            } else {
                try {
                    toFile();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        iters++;
    }

    /**
     * Get the geographic information of a node
     *
     * @param ipaddr the IP address to get the geographic information of
     * @return a tuple containing the location's name, latitude, and longitude
     */
    private Triple<String, Double, Double> getGeo(String ipaddr) {
        try {
            InetAddress i = Inet4Address.getByName(ipaddr);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
            buffer.put(new byte[]{0, 0, 0, 0});
            buffer.put(i.getAddress());
            buffer.position(0);
            long converted = buffer.getLong();
            return geo.get(converted);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get the shortest distance from src node to dst node
     *
     * @param src the node to get the distance from
     * @param dest the node to get the distance to
     * @return the shortest distance from src node to dst node, or 9999 if it is not reachable
     */
    private int getHopsFrom(Node src, Node dest) {
        Queue<Node> toExplore = new LinkedList<>();
        Map<Node, Integer> distances = new HashMap<>();
        distances.put(src, 0);
        toExplore.add(src);

        while (!toExplore.isEmpty()) {
            Node next = toExplore.poll();
            if (next.equals(dest)) {
                return distances.get(next);
            }
            for (Node neighbour : graph.adjacentNodes(next)) {
                if (distances.get(neighbour) == null) {
                    distances.put(neighbour, distances.get(next) + 1);
                    toExplore.add(neighbour);
                }
            }
        }

        return 9999;
    }

    /**
     * Transforms the current graph state into a SQL INSERT query
     *
     * @return a SQL query
     */
    private String getSql() {
        StringBuilder sb = new StringBuilder();

        Output out = new Output();
        OutputWithLocation locs = new OutputWithLocation();
        Map<String, Integer> locIds = new HashMap<>();
        locIds.put("", -1);

        sb.append("DELETE FROM mappy.EthereumConnection;\n");
		sb.append("DELETE FROM mappy.EthereumNode;\n");
		sb.append("DELETE FROM mappy.EthereumLocation;\n");

        sb.append("INSERT INTO mappy.EthereumLocation (loc, lat, long, name, density) VALUES\n");
        for(int i = 0; i < locs.nodes.size(); i++) {
            sb.append("(");
            sb.append(i).append(", ");
            LocationOutput loc = locs.nodes.get(i);
            sb.append(loc.latitude).append(", ");
            sb.append(loc.longitude).append(", ");
            sb.append("'").append(loc.id).append("', ");
            sb.append(loc.density).append("),\n");
            locIds.put(loc.id, i);
        }
        sb.deleteCharAt(sb.lastIndexOf("\n"));
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(";\n");

        sb.append("INSERT INTO mappy.EthereumNode (id, ip, loc) VALUES\n");
        for(NodeOutput node : out.nodes) {
            sb.append("(");
            sb.append("'").append(node.id).append("', ");
            sb.append("'").append(node.ip).append("', ");
            sb.append(locIds.get(node.location)).append("),\n");
        }
        sb.deleteCharAt(sb.lastIndexOf("\n"));
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(";\n");

        sb.append("INSERT INTO mappy.EthereumConnection (neighbour, node) VALUES\n");
        for(LinkOutput link : out.links) {
            sb.append("(");
            sb.append("'").append(link.source).append("', ");
            sb.append("'").append(link.target).append("'),\n");
        }
        sb.deleteCharAt(sb.lastIndexOf("\n"));
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(";\n");

		logger.info(sb.toString());
        return sb.toString();
    }

    /**
     * Write to database
     *
     * TODO: In progress
     */
    private void toDb() {
        connectToDb();

        if(conn == null) {
            logger.error("Cannot write to DB due to bad connection");
        }

        try {
            Statement stmt = conn.createStatement();
            String sql = getSql();
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Cannot write to DB");
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write the current graph to a file
     *
     * @throws IOException when file IO errors ocur
     */
    private void toFile() throws IOException {
        Gson gson = new Gson();

        Output out = new Output();
        OutputWithLocation locs = new OutputWithLocation();

        try (BufferedWriter nodeFile = new BufferedWriter(new FileWriter(NODE_FILE))) {
            nodeFile.write(gson.toJson(out));
        }
        try (BufferedWriter locFile = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            locFile.write(gson.toJson(locs));
        }
    }

    /**
     * Represents a simple JSON output containing nodes and links
     */
    private class Output {
        private List<NodeOutput> nodes;
        private List<LinkOutput> links;

        private void addNode(Set<NodeOutput> nodes, Set<String> hexIds, Node node) {
            Triple<String, Double, Double> homeGeo = getGeo(node.getHost());
            String geoName;
            if(homeGeo == null) {
                geoName = "";
            } else {
                geoName = homeGeo.getLeft();
            }
            nodes.add(new NodeOutput(node.getHexId(), node.getHost() + ":" + node.getPort(),
                    geoName, getHopsFrom(manager.homeNode, node) + 1));
            hexIds.add(node.getHexId());
        }

        private Output() {
            Set<NodeOutput> nodes = new HashSet<>();
            Set<LinkOutput> links = new HashSet<>();
            Set<String> hexIds = new HashSet<>();

            //addNode(nodes, hexIds, manager.homeNode);
            for (Node node : Graphs.reachableNodes(graph, manager.homeNode)) {
                addNode(nodes, hexIds, node);
            }

            for (EndpointPair<Node> pair : graph.edges()) {
                if (hexIds.contains(pair.nodeU().getHexId()) && hexIds.contains(pair.nodeV().getHexId())) {
                    links.add(new LinkOutput(pair.nodeU().getHexId(), pair.nodeV().getHexId()));
                }
            }

            logger.info("WRITING " + nodes.size() + " NODES & " + links.size() + " LINKS TO FILE");
            this.nodes = new ArrayList<>(nodes);
            this.links = new ArrayList<>(links);
        }
    }

    /**
     * Represents a condensed JSON output (nodes are mapped to locations)
     */
    private class OutputWithLocation {
        private List<LocationOutput> nodes;
        private List<LinkOutput> links;

        private String addLocation(Map<String, LocationOutput> nodes, Node node) {
            Triple<String, Double, Double> geoLoc = getGeo(node.getHost());
            if (geoLoc == null) {
                return null;
            }
            if (!nodes.containsKey(geoLoc.getLeft())) {
                nodes.put(geoLoc.getLeft(), new LocationOutput(geoLoc.getLeft(), geoLoc.getMiddle(), geoLoc.getRight()));
            }
            nodes.get(geoLoc.getLeft()).addNode(node);
            return geoLoc.getLeft();
        }

        private OutputWithLocation() {
            Map<String, LocationOutput> nodes = new HashMap<>();
            Map<String, String> idToLoc = new HashMap<>();
            Set<LinkOutput> links = new HashSet<>();

            for (Node node : Graphs.reachableNodes(graph, manager.homeNode)) {
                String loc = addLocation(nodes, node);
                if (loc != null) {
                    idToLoc.put(node.getHexId(), loc);
                }
            }
            //String loc = addLocation(nodes, manager.getTable().getNode());
            //idToLoc.put(manager.homeNode.getHexId(), loc);

            for (EndpointPair<Node> pair : graph.edges()) {
                if (idToLoc.containsKey(pair.nodeU().getHexId()) && idToLoc.containsKey(pair.nodeV().getHexId())) {
                    links.add(new LinkOutput(idToLoc.get(pair.nodeU().getHexId()),
                            idToLoc.get(pair.nodeV().getHexId())));
                }
            }

            logger.info("WRITING " + nodes.size() + " LOCATIONS TO FILE");
            this.nodes = new ArrayList<>(nodes.values());
            this.links = new ArrayList<>(links);
        }
    }

    /**
     * Represents the JSON output of a single node
     */
    private class NodeOutput {
        private String id;
        private String ip;
        private String location;
        private int distance; //distance from home node

        public NodeOutput(String id, String ip, String location, int distance) {
            this.id = id;
            this.ip = ip;
            this.location = location;
            this.distance = distance;
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

    /**
     * Represents the JSON output of a single location
     */
    private class LocationOutput {
        private String id;
        private double latitude;
        private double longitude;
        private int density;
        private List<NodeOutput> nodes;

        public LocationOutput(String id, double latitude, double longitude) {
            this.id = id;
            this.latitude = latitude;
            this.longitude = longitude;
            this.nodes = new ArrayList<>();
            this.density = 0;
        }

        private void addNode(Node node) {
            density += 1;
            nodes.add(new NodeOutput(node.getHexId(), node.getHost() + ":" + node.getPort(), id, 1));
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

    /**
     * Represents the JSON output of a single link
     */
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
}
