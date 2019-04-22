package org.ethereum.net.rlpx.discover;

import org.ethereum.net.rlpx.Node;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Crawler extends Thread {
    private static Crawler instance; //singleton instance because I'm lazy

    private static final int PAUSE = 10;

    private Set<Node> all; //all the nodes we have traversed - going to get big so may have to reconsider if this is appropriate
    private Set<GraphNode> closest; //graph nodes of the nodes connected to our node
    private byte[] myId; //id of our node

    private NodeManager manager; //used to do all the important networky things

    private boolean active; //set this to false when we want to stop traversal
    private final Object lock = new Object(); //thread safety is important

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
        this.closest = new HashSet<>();
        for(Node node : this.all) {
            closest.add(new GraphNode(node));
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
        crawl();
    }

    /**
     * Do all the important things
     */
    private void crawl() {
        active = true;

        while(active) {
            synchronized (lock) {
                for (Node node : all) {
                    manager.getNodeHandler(node).sendFindNode(myId);
                }

                try {
                    Thread.sleep(PAUSE); //take a breather to try to avoid overloading the network
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void stopCrawl() {
        active = false;
    }

    public void addNodes(List<Node> nodes) {
        synchronized (lock) {
            logger.info("Received nodes: " + nodes.toString());
            this.all.addAll(nodes);
        }
    }

    /**
     * Get the nodes closest to our node
     *
     * @return A list of nodes adjacent to our node
     */
    public Set<GraphNode> getClosest() {
        return closest;
    }

    /**
     * Little subclass here to represent a node in the topology graph
     */
    private class GraphNode {
        private Node node;
        private List<Node> neighbours;

        public GraphNode(Node node) {
            this.node = node;
            this.neighbours = new ArrayList<>();
        }
    }
}
