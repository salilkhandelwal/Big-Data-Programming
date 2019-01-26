// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

 
public class RedditAverage extends Configured implements Tool {
 
    public static class RedditMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{

       private LongPairWritable pair = new LongPairWritable();
       private Text sreddit = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	 
          JSONObject record = new JSONObject(value.toString());

        	
          String sredditString = (String) record.get("subreddit");
          int score = (Integer) record.get("score");
            
          sreddit.set(sredditString);
    	  pair.set(1, score);
    	  context.write(sreddit, pair);
            

        }

    }

 
 public static class RedditCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

    	private LongPairWritable pair = new LongPairWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            
            long count = 0;
            long totalscore = 0;

            for (LongPairWritable val : values) {
            	
                count = count + val.get_0();
            	totalscore = totalscore + val.get_1();
 
            }
             
             pair.set(count, totalscore);
             context.write(key, pair);

         
        }
    
   }

  public static class RedditReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            
            long count = 0;
            long totalscore = 0;

            for (LongPairWritable val : values) {
            	
                count = count + val.get_0();
            	totalscore = totalscore + val.get_1();

            }
            
            
            double avg = 0;
            avg = (double)totalscore/(double)count;
            
            result.set(avg);
            context.write(key, result);
        
         }
    
   }

    public static void main(String[] args) throws Exception {
        
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);

    }
 
    @Override
    public int run(String[] args) throws Exception {
        
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average and json input");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(RedditMapper.class);
        job.setCombinerClass(RedditCombiner.class);
        job.setReducerClass(RedditReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;

    }

}

