import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class WikipediaPopular extends Configured implements Tool {
 
    public static class PageViewMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        private LongWritable count = new LongWritable();
        private Text datetime = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            
            String timestamp = "";
            String lang = "";
            long noVisit = 0;
            String size = "";
            String title="";
            
       
            timestamp = split[0];
            lang = split[1];
            title = split[2];
            noVisit = Long.parseLong(split[3]);
            size = split[4];
            
             
            	if (lang.startsWith("en")) {
            		if (!title.equalsIgnoreCase("Main_Page") && !title.startsWith("Special:")) 


                        {
            			String filename = timestamp;
            			datetime.set(filename);
            			count.set(noVisit);
            			context.write(datetime, count);
            		}
            	}
            }
        }
    
 
    public static class MaxPageViewReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable ans = new LongWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {

            long max = 0;
            for (LongWritable val : values) {

            	if (val.get() > max) {

            		max = val.get();

            	}
            }

            ans.set(max);

            context.write(key, ans);
        }
    }
 
    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);

    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Wikipedia popularity");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(PageViewMapper.class);
        job.setCombinerClass(MaxPageViewReducer.class);
        job.setReducerClass(MaxPageViewReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
