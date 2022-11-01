

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class productsentiment {

  public static class ScoreMapper 
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text ID = new Text();
    private IntWritable sentiment=new IntWritable();
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
    	String line = value.toString();
    	if(line.charAt(0)!='u') 
    	{
	        String[] line_values = line.split(",");
	        ID.set(line_values[1]);
	        sentiment.set(Integer.parseInt(line_values[2]));
	        context.write(ID, sentiment);
    	}
    }
  }

  public static class AverageReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context
    ) throws IOException, InterruptedException {
      int sum=0;
      int count=0;
      for (IntWritable val : values) {
        sum += val.get();
        count++;
      }
      result.set(sum/count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sentiment score");
    job.setJarByClass(productsentiment.class);
    job.setMapperClass(ScoreMapper.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}