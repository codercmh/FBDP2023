package cmh_e2;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PredBreak {
    public static class PredBreakMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one =new IntWritable(1);
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            String line = value.toString();
            String[] fields = line.split(",");
            if (key.get()!=0){
                String weekday=fields[25].trim();
                context.write(new Text(weekday), one);
            }
        }
    }

    public static class PredBreakReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        TreeMap<Integer,String> sortedWeekdays = new TreeMap<>(Collections.reverseOrder());
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum=0;
            for (IntWritable val:values){
                sum +=val.get();
            }
            sortedWeekdays.put(sum,key.toString());
            //context.write(key,new IntWritable(sum));
        } 
        protected void cleanup(Context context) throws IOException,InterruptedException{
            for (Integer count : sortedWeekdays.keySet()) {
                context.write(new Text(sortedWeekdays.get(count)), new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PredBreak");

        job.setJarByClass(PredBreak.class);
        job.setMapperClass(PredBreakMapper.class);
        job.setCombinerClass(PredBreakReducer.class);
        job.setReducerClass(PredBreakReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);   

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
