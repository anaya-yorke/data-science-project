//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class HadoopDriver {
    public HadoopDriver() {
    }

    public static void main(String[] args) throws Exception {
        JobClient myClient = new JobClient();
        JobConf jobConf = new JobConf(HadoopDriver.class);
        jobConf.setJobName("SeaLevelTrendAnalysis");
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(HadoopMap.class);
        jobConf.setReducerClass(HadoopReduce.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobConf, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        myClient.setConf(jobConf);

        try {
            JobClient.runJob(jobConf);
        } catch (Exception var4) {
            var4.printStackTrace();
        }

    }
}
