package org.ethereum.net.rlpx.discover;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Geolocator {
    private static final String DATABASE = "files/mapping.csv";

    public static RangeMap<Integer, Triple<String, Double, Double>> getDatabase() {
        RangeMap<Integer, Triple<String, Double, Double>> result = TreeRangeMap.create();
        try (BufferedReader in = new BufferedReader(new FileReader(DATABASE))) {
            String line = in.readLine();
            while(line != null) {
                String[] fields = line.split(",");
                try {
                    Integer startAddr = Integer.parseInt(fields[0]);
                    Integer endAddr = Integer.parseInt(fields[1]);
                    Double latitude = Double.parseDouble(fields[6]);
                    Double longitude = Double.parseDouble(fields[7]);
                    String name = fields[5] + " (" + 2 + ")";
                    result.put(Range.closed(startAddr, endAddr), new ImmutableTriple<>(name, latitude, longitude));

                } catch (NumberFormatException | IndexOutOfBoundsException ex) {
                    ex.printStackTrace();
                }
                line = in.readLine();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return result;
    }
}
