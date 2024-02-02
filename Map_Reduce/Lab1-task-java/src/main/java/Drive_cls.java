import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
public class Drive_cls {
    public static void main(String[] args) throws IOException{
        // Create a Hadoop job configuration
        JobConf conf = new JobConf(Drive_cls.class);
        conf.setJobName("avg_c_temp");
        // Specify the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
        // Set the mapper, combiner, and reducer classes
        conf.setMapperClass(Mapper_cls.class);
        conf.setCombinerClass(Reducer_cls.class);
        conf.setReducerClass(Reducer_cls.class);
        // Specify the input and output formats
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // Set the input and output paths
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        // Run the Hadoop job
        JobClient.runJob(conf);
    }
}