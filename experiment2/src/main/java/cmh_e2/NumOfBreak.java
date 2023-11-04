package cmh_e2;

import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.File;
import java.io.FileInputStream;
//import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NumOfBreak{
    public static class NumOfBreakMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one =new IntWritable(1);
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            String line = value.toString();
            String[] fields = line.split(",");
            if (key.get()!=0){
                String target=fields[fields.length-1].trim();
                context.write(new Text(target), one);
            }
        }
    }

    public static class NumOfBreakReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum=0;
            for (IntWritable val:values){
                sum +=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NumOfBreak");

        job.setJarByClass(NumOfBreak.class);
        job.setMapperClass(NumOfBreakMapper.class);
        job.setReducerClass(NumOfBreakReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);   

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
