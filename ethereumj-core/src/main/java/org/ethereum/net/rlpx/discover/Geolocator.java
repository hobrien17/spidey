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

    public static RangeMap<Long, Triple<String, Double, Double>> getDatabase() {
        RangeMap<Long, Triple<String, Double, Double>> result = TreeRangeMap.create();
        try (BufferedReader in = new BufferedReader(new FileReader(DATABASE))) {
            String line = in.readLine();
            while(line != null) {
                String[] fields = line.split(",(?=([^\"]|\"[^\"]*\")*$)");
				// hi ive actually no clue what this regex does but it splits the csv up nicely
                try {
                    Long startAddr = Long.parseLong(fields[0].replaceAll("\"", ""));
                    Long endAddr = Long.parseLong(fields[1].replaceAll("\"", ""));
                    Double latitude = Double.parseDouble(fields[6].replaceAll("\"", ""));
                    Double longitude = Double.parseDouble(fields[7].replaceAll("\"", ""));
                    String name = fields[5].replaceAll("\"", "") + " (" + fields[2].replaceAll("\"", "") + ")";
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
