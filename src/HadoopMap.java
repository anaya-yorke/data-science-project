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

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] data = line.split(",");  // Splitting CSV line

        // Assuming the year is the second column and sea level is the third column
        if (data.length > 2) {
            String location = data[0].trim();
            String year = data[1].trim();
            String seaLevel = data[2].trim();

            Text outputKey = new Text(location);
            Text outputValue = new Text(year + "," + seaLevel);

            output.collect(outputKey, outputValue);
        }
    }
}
