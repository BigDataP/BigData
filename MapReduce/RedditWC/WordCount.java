package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;

import java.util.List;
import java.util.LinkedList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.IOException;


public class WordCount extends Configured implements Tool
{
    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable minusone = new IntWritable(-1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();
        
        List<Date> datesasc = new LinkedList<Date>();
        List<Date> datesdesc = new LinkedList<Date>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
           
            getDates(localFiles[0].getName(), true);
            getDates(localFiles[1].getName(), false);
        }
            
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String date = value.toString().substring(0,10);
            String[] words = value.toString().substring(10, value.toString().length()).split(" ") ;
            
            IntWritable num = getWritableVal(date);
            
            for (String str: words)
            {
                word.set(str);
                context.write(word, num);  //Write map result {word,1}
            }
        }
        private IntWritable getWritableVal(String date) {
            try {
                Date d = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                return datesasc.contains(d) ? one : datesdesc.contains(d) ? minusone : zero;
            } catch (Exception ex) {
                System.err.println("[DATE PARSING] Exception while reading stop words file: " + ex.getMessage());
                return null;
            }
        }
        private void getDates(String filePath, boolean asc) {
            try {
                System.out.println("[Words] READING FILE --> " + filePath);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String date = null;
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

                while((date = bufferedReader.readLine()) != null) {
                    if (asc) {
                        datesasc.add(df.parse(date));
                    } else {
                        datesdesc.add(df.parse(date));
                    } 
                }
            } catch (Exception ex) {
                System.err.println("[Words] Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }

    public static class WordCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            int total = 0;
            for (IntWritable val : values) {
                total++ ;
            }
            context.write(key, new IntWritable(total));  // Write reduce result {word,count}
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        DistributedCache.addCacheFile(new URI(args[2]), conf);
        DistributedCache.addCacheFile(new URI(args[3]), conf);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}

