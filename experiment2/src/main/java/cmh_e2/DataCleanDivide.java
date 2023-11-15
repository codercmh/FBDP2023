package cmh_e2;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class DataCleanDivide {
    public static class DataCleanDivideMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Random random = new Random();
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            if (key.get()!=0){
                if (random.nextDouble() < 0.8){
                    context.write(new Text("train"), value);
                }
                else{
                    context.write(new Text("test"), value);
                }
            }
        }
    }

    public static class DataCleanDivideReducer extends Reducer<Text,Text,NullWritable,Text>{
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        protected void setup(Context context) {
            // 初始化MultipleOutputs对象
            multipleOutputs = new MultipleOutputs<>(context);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 根据key的值将数据写入不同的文件
            for (Text value : values) {
                multipleOutputs.write(NullWritable.get(), value, key.toString() + "/part");
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 关闭MultipleOutputs对象
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DataCleanDivide");

        job.setJarByClass(DataCleanDivide.class);
        job.setMapperClass(DataCleanDivideMapper.class);
        job.setCombinerClass(DataCleanDivideReducer.class);
        job.setReducerClass(DataCleanDivideReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);   

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
