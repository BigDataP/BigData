
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashtagSentiments extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.err.println("Usage: Sentiments <input folder> <output folder> <PositiveWordsFile> <NegastiveWordsFile>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }

        //Positive words
        DistributedCache.addCacheFile(new URI(args[2]), conf);
        //job.addCacheFile(new Path(args[2]).toUri()); --> Does not work
        //Negative words
        DistributedCache.addCacheFile(new URI(args[3]), conf);
        //job.addCacheFile(new Path(args[3]).toUri());  --> Does not work

        Job job = new Job(conf, "Hashtag sentiments");
        job.setJarByClass(HashtagSentiments.class);

        job.setMapperClass(SentimentCountMapper.class);
        job.setReducerClass(TopNReducer.class);

        FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HashtagSentiments(), args);
        System.exit(res);
    }

    public static class SentimentCountMapper extends Mapper<Object, Text, Text, IntArrayWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static Set positiveWords = new HashSet();
        private static Set negativeWords = new HashSet();
        final static Pattern TAG_PATTERN = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
            //We load positive words into one set and negative words in another one
            readWordsFile(localFiles[0].getName(),1);
            readWordsFile(localFiles[1].getName(),0);
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            List<String> hashtags = new ArrayList();

            int sentimentSum = 0;

            Matcher matcher = TAG_PATTERN.matcher(value.toString());
            while (matcher.find()) {
                String found = matcher.group();
                found = found.trim();
                found=found.replace("#","");
                if (positiveWords.contains(found.toLowerCase())) {
                    sentimentSum += 1;
                    hashtags.add(found.toLowerCase());
                } else if (negativeWords.contains(found.toLowerCase())) {
                    sentimentSum -= 1;
                    hashtags.add(found.toLowerCase());
                }
            }

            //For each hashtag we emit the hashtag and the overall sentiment of the tweet
            for(String hashtag:hashtags) {
                context.write(new Text(hashtag), new IntArrayWritable(
                        new Pair(new IntWritable(sentimentSum),new IntWritable(0))));
            }
        }

        private static void readWordsFile(String filePath, int positive) {
            try {
                System.out.println("[Words] READING FILE --> " + filePath);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String word = null;
                //I distinguish between add the words into positive of negative words set with the variable positive
                //(0- negative and 1-positive)
                while((word = bufferedReader.readLine()) != null) {
                    if (!word.startsWith(";")) {
                        if (positive == 1) {
                            positiveWords.add(word.toLowerCase());
                        } else {
                            negativeWords.add(word.toLowerCase());
                        }
                    }
                }
            } catch (Exception ex) {
                System.err.println("[Words] Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }

    /*
    * This class have been created in order to be able to create a data structure like a pair to represent the
    * two necessary things for the reducer by a hashtag -> global sentiment of the tweet and the length of the tweet
    */

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Pair<Writable, Writable> pair) {
            super(IntWritable.class, new IntWritable[] { (IntWritable) pair.getFirst(),
                    (IntWritable) pair.getSecond() });
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");

            for (String s : super.toStrings()) {sb.append(s).append(" ");}

            sb.append("]");
            return sb.toString().replace(" ]","]").replace(" ",",");
        }
    }


    public static class TopNReducer extends Reducer <Text, IntArrayWritable,Text, IntArrayWritable>{

        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            int length;
            int sentiment;
            int positive = 0;
            for(IntArrayWritable value :values){
                Pair pair = new Pair<Writable, Writable>(value.get()[0], value.get()[1]);
                sentiment = ((IntWritable)pair.getFirst()).get();
                length = ((IntWritable)pair.getSecond()).get();
                sum +=(length-sentiment)*length;
            }

            //We write the hashtag and the sentiment and then 0 for all cases. As bigger is the sentiment, it means that
            //the hashtag is in a negative sentiment
            context.write(new Text(key), new IntArrayWritable(new Pair(new IntWritable(sum),new IntWritable(0))));
        }
    }
}

