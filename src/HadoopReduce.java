//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//4.	Create the Reducer – Do not name this Reducer.
//a.	Extends MapReduceBase and implements Reducer<Text, Text, Text, Text>
//b.	Implement the reduce method.
//c.	Create a process that collects the links for each key and sends them to the OutputCollector.

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.*;


public class HadoopReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Prepare data for trend analysis
        List<Float> years = new ArrayList<>();
        List<Float> seaLevels = new ArrayList<>();

        while (values.hasNext()) {
            String[] valueParts = values.next().toString().split(",");
            try {
                float year = Float.parseFloat(valueParts[0]);
                float seaLevel = Float.parseFloat(valueParts[1]);
                years.add(year);
                seaLevels.add(seaLevel);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format: " + e.getMessage());
            }
        }

        // Compute trend (e.g., using linear regression)
        // ... Compute slope and intercept here ...

        // For simplicity, let's just calculate average sea level
        float sumSeaLevel = 0;
        for (Float seaLevel : seaLevels) {
            sumSeaLevel += seaLevel;
        }
        float average = sumSeaLevel / seaLevels.size();

        // Emit average sea level for the location
        output.collect(key, new Text(String.valueOf(average)));
    }
}

