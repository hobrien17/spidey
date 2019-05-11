package org.ethereum.net.rlpx.discover;

import com.google.common.graph.Graph;
import org.ethereum.net.rlpx.NeighborsMessage;
import org.ethereum.net.rlpx.Node;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.*;

public class Crawler extends Thread {
    private static Crawler instance; //singleton instance because I'm lazy

    private static final int PAUSE = 5;
	private static final int WRITE_INTERVAL = 600;

    private Set<Node> all; //all the nodes we have traversed - going to get big so may have to reconsider if this is appropriate
    private Set<Node> toAdd; //all the nodes we have found but need to be added to all

    private Map<String, GraphNode> graph; //all the nodes we have traversed as graph nodes
    private byte[] myId; //id of our node

    private NodeManager manager; //used to do all the important networky things
	private NodeFileWriter writer;

	private int iters;
    boolean active; //set this to false when we want to stop traversal
    final Object lock = new Object(); //thread safety is important

    static final org.slf4j.Logger logger = LoggerFactory.getLogger("discover");

    /**
     * Constructs a new Crawler object
     *
     * @param manager the client's node manager
     * @param myId the ID of the node we are running
     */
    public Crawler(NodeManager manager, byte[] myId) {
        this.myId = myId;
        this.manager = manager;
        this.all = new HashSet<>(manager.getTable().getClosestNodes(myId));
        this.toAdd = new HashSet<>();
        this.graph = new HashMap<>();
		this.writer = new NodeFileWriter();
		this.iters = 1;
        for(Node node : this.all) {
            graph.put(node.getHost() + ":" + node.getPort(), new GraphNode(node));
        }
    }

    /**
     * Assign a new Crawler object to our singleton instance
     *
     * @param manager the client's node manager
     * @param myId the ID of the node we are running
     */
    public static void setup(NodeManager manager, byte[] myId) {
        instance = new Crawler(manager, myId);
    }

    /**
     * Get the current singleton instance
     *
     * @return the current singleton instance
     */
    public static Crawler get() {
        return instance;
    }

    @Override
    public void run() {
		active = true;
		System.out.println("Working Directory = " +
              System.getProperty("user.dir"));
        crawl();
    }

    /**
     * Do all the important things
     */
    private void crawl() {
        while(active) {
            for (Node node : all) {
                manager.getNodeHandler(node).sendFindNode(myId);

                try {
                    Thread.sleep(PAUSE); //take a breather to try to avoid overloading the network
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            synchronized (lock) {
                all.addAll(toAdd);
                toAdd = new HashSet<>();
            }
        }
    }

    public void stopCrawl() {
        active = false;
    }

    public void addNodes(DiscoveryEvent evt) {
        Collection<Node> nodes = ((NeighborsMessage)evt.getMessage()).getNodes();
        List<GraphNode> graphNodes = new ArrayList<>();

        for(Node node : nodes) { //convert each Node to a GraphNode and add it to the graph
            GraphNode graphNode = new GraphNode(node);
            graphNodes.add(graphNode);
            graph.put(node.getHost() + ":" + node.getPort(), graphNode);
        }

        GraphNode target =  graph.get(evt.getAddress().getAddress().getHostAddress() + ":" + evt.getAddress().getPort());
        if(target != null) { // quick fix to stop NPE, alternative approach could be to add this node to the graph
            target.neighbours = graphNodes;
        }

        synchronized (lock) {
            this.toAdd.addAll(nodes);
        }
        logger.info("" + graph.size());
		
		if(iters % WRITE_INTERVAL == 0) {
			logger.info("WRITING GRAPH TO FILE " + iters + " " + WRITE_INTERVAL);
			try {
				synchronized (lock) {
					writer.write(getGraphRoot());
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		iters++;
    }

    /**
     * Get the nodes closest to our node
     *
     * @return A list of nodes adjacent to our node
     */
    public Map<String, GraphNode> getGraph() {
        return new HashMap<>(graph);
    }

    public GraphNode getGraphRoot() {
        Node myNode = manager.getTable().getNode();
        GraphNode root = new GraphNode(myNode);

        for(Node node : manager.getTable().getClosestNodes(myId)) {
            GraphNode neighbour = graph.get(node.getHost() + ":" + node.getPort());
            root.neighbours.add(neighbour);
        }

        return root;
    }

    /**
     * Little subclass here to represent a node in the topology graph
     */
    class GraphNode {
        Node node;
        List<GraphNode> neighbours;

        GraphNode(Node node) {
            this.node = node;
            this.neighbours = new ArrayList<>();
        }

        @Override
        public String toString() {
            return node.toString();
        }
    }
}
