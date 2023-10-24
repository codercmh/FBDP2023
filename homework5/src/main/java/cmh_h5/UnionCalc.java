package cmh_h5;

import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.File;
import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;

public class UnionCalc{
        public static class UnionCalcMapper extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); 

            String[] fields = line.split(",");
    
            if (fields.length >= 2) {
                String index = fields[0].trim();
                String stockCode = fields[1].trim();
                context.write(new Text(index), new Text(stockCode));
            }
        }
    }

    public static class UnionCalcReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> stockCodeSet = new HashSet<>();
            
            // 遍历某个index下所有相同成分股代码的记录，将其添加到集合中
            for (Text value : values) {
                stockCodeSet.add(value.toString());
            }
            
            // 输出去除重复后的记录
            for (String stockCode : stockCodeSet) {
                context.write(key, new Text(stockCode));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UnionCalc");

        job.setJarByClass(UnionCalc.class);
        job.setMapperClass(UnionCalcMapper.class);
        job.setReducerClass(UnionCalcReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // 输入文件A和B的路径input
    
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 输出文件路径output
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

