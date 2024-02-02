import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper_cls extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] tokens = line.split(",");

        if (tokens.length >= 2) {
            String city = tokens[0].trim();
            String f_tempStr = tokens[1].trim();

            try {
                // convert temperature from Fahrenheit to Celsius
                float f_temp = Float.parseFloat(f_tempStr);
                float c_temp = (f_temp - 32) * 5.0f / 9.0f;

                output.collect(new Text(city), new FloatWritable(c_temp));
            } catch (NumberFormatException e) {
                // ignore if temperature was not a valid float
            }
        }
    }
}

