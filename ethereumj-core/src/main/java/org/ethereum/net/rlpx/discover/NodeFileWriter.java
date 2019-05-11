package org.ethereum.net.rlpx.discover;

import com.google.common.collect.RangeMap;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

public class NodeFileWriter extends AbstractWriter {
    private final static String NODE_FILE = "files/out/nodes.json";
    private final static String LINKS_FILE = "files/out/links.json";
    private final static String LOCATION_FILE = "files/out/locations.json";

    private Set<NodeOutput> nodes;
    private Set<LinkOutput> links;
    private Set<LocationOutput> locations;

    private RangeMap<Integer, Triple<String, Double, Double>> geo;

    public NodeFileWriter(Crawler master) {
        super(master);
        geo = Geolocator.getDatabase();
    }

    private Triple<String, Double, Double> getGeo(String ipaddr) {
        try {
            InetAddress i = Inet4Address.getByName(ipaddr);
            int converted = ByteBuffer.wrap(i.getAddress()).getInt();
            return geo.get(converted);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void makeOutput() {
        Crawler.GraphNode root = master.getGraphRoot();
        nodes = new HashSet<>();
        links = new HashSet<>();

        Set<Crawler.GraphNode> visited = new HashSet<>();
        Stack<Crawler.GraphNode> toVisit = new Stack<>();
        visited.add(root);
        toVisit.add(root);

        while(!toVisit.isEmpty()) {
            Crawler.GraphNode next = toVisit.pop();
            Triple<String, Double, Double> location = getGeo(next.node.getHost());
            if(location == null) {
                continue;
            }
            LocationOutput loc = new LocationOutput(location.getLeft(), location.getMiddle(), location.getRight());
            nodes.add(new NodeOutput(next.node.getHost(), loc.name));
            locations.add(loc);
            visited.add(next);
            for(Crawler.GraphNode neighbour : next.neighbours) {
                links.add(new LinkOutput(next.node.getHost(), neighbour.node.getHost()));
                if(!visited.contains(neighbour)) {
                    toVisit.push(neighbour);
                }
            }
        }
    }

    @Override
    protected void write() throws IOException {
        synchronized (master.lock) {
            makeOutput();
        }
        Gson gson = new Gson();
        String nodeJson = "var nodes = " + gson.toJson(nodes) + ";";
        String linkJson = "var links = " + gson.toJson(links) + ";";
        String locJson = "var locs = " + gson.toJson(locations) + ";";
        try (BufferedWriter nodeOut = new BufferedWriter(new FileWriter(NODE_FILE))) {
            nodeOut.write(nodeJson);
        }
        try (BufferedWriter linkOut = new BufferedWriter(new FileWriter(LINKS_FILE))) {
            linkOut.write(linkJson);
        }
        try (BufferedWriter linkOut = new BufferedWriter(new FileWriter(LOCATION_FILE))) {
            linkOut.write(locJson);
        }
    }

    private class LocationOutput {
        String name;
        private double latitude;
        private double longitude;

        LocationOutput(String name, double latitude, double longitude) {
            this.name = name;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof LocationOutput)) {
                return false;
            }
            LocationOutput other = (LocationOutput) obj;
            return other.name.equals(this.name) && other.latitude == this.latitude && other.longitude == this.longitude;
        }

        @Override
        public int hashCode() {
            return (int)(17*latitude + 31*longitude + 3*name.hashCode());
        }
    }

    private class NodeOutput {
        private String ip;
        private String loc;

        NodeOutput(String ip, String loc) {
            this.ip = ip;
            this.loc = loc;
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof NodeOutput)) {
                return false;
            }
            NodeOutput other = (NodeOutput)obj;
            return other.ip.equals(this.ip) && other.loc.equals(this.loc);
        }

        @Override
        public int hashCode() {
            return 7*loc.hashCode() + 23*ip.hashCode();
        }
    }

    private class LinkOutput {
        private String srcIp;
        private String dstIp;

        LinkOutput(String srcIp, String dstIp) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof LinkOutput)) {
                return false;
            }
            LinkOutput other = (LinkOutput)obj;
            return (srcIp.equals(other.srcIp) && dstIp.equals(other.dstIp)) ||
                    (dstIp.equals(other.srcIp) && srcIp.equals(other.dstIp));
        }

        @Override
        public int hashCode() {
            return srcIp.hashCode() + dstIp.hashCode();
        }
    }

}
