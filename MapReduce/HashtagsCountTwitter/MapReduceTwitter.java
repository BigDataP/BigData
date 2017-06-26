
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by carol on 20/05/17.
 */

public class MapReduceTwitter extends Configured implements Tool
{
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, LongWritable> {


        final static Pattern TAG_PATTERN = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        private final static LongWritable ONE = new LongWritable(1L);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Matcher matcher = TAG_PATTERN.matcher(value.toString());
            while (matcher.find()) {
                String found = matcher.group();
                found = found.trim();
                word.set(found.toLowerCase());
                context.write(word, ONE);
            }

        }
    }

    public static class LongSumReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class MyInverseMapper
            extends Mapper<Text,Text,IntWritable,Text>
    {
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException
        {
            context.write(new IntWritable(-Integer.parseInt(value.toString())), key);  // Write map result {-count, word}
        }
    }


    public static class MyInverseReducer
            extends Reducer<IntWritable,Text,Text,IntWritable>
    {
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException
        {
            for (Text word : values) {
                context.write(word, new IntWritable(-key.get()));  // Write reduce result {word,count} ORDERED
            }

        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Cleaning intermediate data.. can be ignored.
        FileSystem.get(conf).delete(new Path("/home/carol/tmp/inter/"), true);

        // Create First Job.
        Job job = Job.getInstance(conf, "mapreducetwitter");
        //job.setJarByClass(this.getClass());
        //Job job = Job.getInstance(conf, "MapReduce Twitter job 1");
        job.setJarByClass(MapReduceTwitter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //job.setMapperClass(TwitterMapper.class);
        //job.setCombinerClass(TwitterReducer.class);
        //job.setReducerClass(TwitterReducer.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/home/carol/tmp/inter/"));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(job);

        // Create Second Job
        Job job2 = Job.getInstance(conf);
        job2.setJobName("MapReduce Twitter job 2");

        job2.setJarByClass(WordCount.class);

        job2.setMapperClass(MyInverseMapper.class);
        job2.setReducerClass(MyInverseReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path("/home/carol/tmp/inter/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(job2);

        JobControl jobctrl = new JobControl("JobCtrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        cJob2.addDependingJob(cJob1);

        Thread jobRunnerThread = new Thread(new MapReduceTwitter.JobRunner(jobctrl));
        jobRunnerThread.start();
        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobctrl.stop();

        // Cleaning intermediate data.. can be ignored.
        FileSystem.get(conf).delete(new Path("/home/carol/tmp/inter/"), true);



        return(1);

        //return (job.waitForCompletion(true) ? 0 : 1);
    }


    class JobRunner implements Runnable {
        private JobControl control;

        public JobRunner(JobControl _control) {
            this.control = _control;
        }

        public void run() {
            this.control.run();
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapReduceTwitter(), args);
        System.exit(exitCode);
    }
}
