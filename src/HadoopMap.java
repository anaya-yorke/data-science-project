import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

//    i.	The LongWritable is the key
//    ii.	The first Text is the value being brought in, the line being read.
//    iii.	OutputCollector is going to capture the output from the map job and write it to a file.
//    iv.	Reporter would allow you to report on the progress of the job.
//    c.	Create a variable that will hold each line as it is passed in. This should be a single String since the lines are passed one at a time.
//    d.	Process the lines, the first number is your site, the second is the link.
//    e.	Add these to the OutputCollector using the collect method.



public class HadoopMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] data = line.split(",");  // Splitting CSV line

        if (data.length > 2) {  // Check to ensure array has sufficient data
            String date = data[0].trim();
            String location = data[1].trim();
            String seaLevel = data[2].trim();

            // Using location-date as key to track sea level over time per location
            Text outputKey = new Text(location + ", " + date);
            Text outputValue = new Text(seaLevel);

            output.collect(outputKey, outputValue);
        }
    }
}
