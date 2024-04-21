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

public class HadoopReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        float sum = 0;
        int count = 0;

        while (values.hasNext()) {
            String valueStr = values.next().toString();
            try {
                float value = Float.parseFloat(valueStr);
                sum += value;
                count++;
            } catch (NumberFormatException e) {
                // Log the error or handle the incorrect data format
                System.err.println("Invalid number format at key " + key + ": " + valueStr);
            }
        }

        if (count > 0) {
            float average = sum / count;
            // Collecting average sea level by location-date
            output.collect(key, new Text(String.valueOf(average)));
        }
    }
}

