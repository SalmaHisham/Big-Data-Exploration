import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Reducer_cls  extends MapReduceBase implements Reducer<Text,FloatWritable,Text,FloatWritable> {
    public void reduce(Text key, Iterator<FloatWritable> values,OutputCollector<Text,FloatWritable> output, Reporter reporter) throws IOException {
        float total_temp=0;
        int cnt = 0;
        while (values.hasNext()) {
            total_temp+=values.next().get();
            cnt ++;
        }
        if (cnt > 0) {
            float averageTemp = total_temp / cnt;
            output.collect(key,new FloatWritable(averageTemp));

        }
    }
}
