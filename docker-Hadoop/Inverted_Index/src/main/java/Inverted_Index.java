import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Inverted_Index {

    // Step 1: Mapper Class
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> stopwords = new HashSet<>();

        // Step 1a: Setup method to read cached file for HDFS
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            // Check if skipping patterns is required
            if (conf.getBoolean("skip.patterns", false)) {
                URI[] localPaths = context.getCacheFiles();
                Path path = new Path(localPaths[0]);

                try (FileSystem fs = FileSystem.get(context.getConfiguration());
                     FSDataInputStream in = fs.open(path);
                     BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                    String pattern;
                    // Adding stopwords to the hashset
                    while ((pattern = br.readLine()) != null) {
                        stopwords.add(pattern);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '" + path);
                }
            }
        }

        // Step 1b: Map method for processing input data
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            // Preprocess the line and tokenize
            String line = value.toString().replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness|.|,|\\?|'|:|;) ", " ").toLowerCase();

            StringTokenizer tokenizer = new StringTokenizer(line);
            // Emit (word, fileName) pairs
            while (tokenizer.hasMoreTokens()) {
                String wordText = tokenizer.nextToken();
                if (stopwords.contains(wordText))
                    continue;
                context.write(new Text(wordText), new Text(fileName));
            }
        }
    }

    // Step 2: Reducer Class
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        // Step 2a: Reduce method to process intermediate key-value pairs
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stb = new StringBuilder();
            HashMap<String, Integer> fileFreq = new HashMap<>();

            // Count occurrences of each file for the given word
            for (Text val : values) {
                Integer count = fileFreq.get(val.toString());
                if (count == null) {
                    count = 0;
                }
                fileFreq.put(val.toString(), count + 1);
            }
            // Emit (word, fileFreq) pairs
            context.write(key, new Text(fileFreq.toString()));
        }
    }

    // Step 3: Main Driver Class
    public static void main(String[] args) throws Exception {
        // Create a Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");

        // Step 3a: Set the main class for the job
        job.setJarByClass(Inverted_Index.class);

        // Step 3b: Set the mapper and reducer classes
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // Step 3c: Set input and output format and key-value classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Step 3d: Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Step 3e: Add distributed cache file if needed
        if (args.length > 2 && "-skip".equals(args[2])) {
            job.getConfiguration().setBoolean("skip.patterns", true);
            job.addCacheFile(new URI(args[3]));
        }

        // Step 3f: Exit the job based on success or failure
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
