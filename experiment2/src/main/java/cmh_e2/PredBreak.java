package cmh_e2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PredBreak {

    public static class PredBreakMapper extends Mapper<LongWritable,Text,Text,Text>{
        private List<String> trainingData = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles(); // 获取缓存文件的URI
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0]);
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    boolean firstLine = true;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (firstLine) {
                            firstLine = false; // 跳过第一行
                            continue;
                        }
                        trainingData.add(line);
                    }
                } catch (IOException e) {
                    System.err.println("Exception reading file from cache: " + e);
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] testSample = value.toString().split(",");
            if ("SK_ID_CURR".equals(testSample[0])){
                return;
            }
            PriorityQueue<Map.Entry<String, Double>> kNearestNeighbors = new PriorityQueue<>(Comparator.comparing(Map.Entry<String, Double>::getValue).reversed());

            for (String trainSample : trainingData) {
                String[] trainData = trainSample.split(",");
                double similarity = calculateSimilarity(testSample, trainData);
                kNearestNeighbors.add(new AbstractMap.SimpleEntry<>(trainData[trainData.length - 1], similarity));

                if (kNearestNeighbors.size() > 5) { // Assuming k=5
                    kNearestNeighbors.poll();
                }
            }
            // Calculate weighted vote
            double weightedSum = 0;
            double similaritySum = 0;
            for (Map.Entry<String, Double> neighbor : kNearestNeighbors) {
                weightedSum += Double.parseDouble(neighbor.getKey()) * neighbor.getValue();
                similaritySum += neighbor.getValue();
            }
            double predictedValue = weightedSum / similaritySum;
            if (predictedValue > 0.5){
                predictedValue = 1;
            }else{
                predictedValue = 0;
            }
            context.write(new Text(testSample[0]), new Text(String.valueOf(predictedValue)+","+testSample[testSample.length-1]));
        }
        private double calculateSimilarity(String[] testSample, String[] trainSample) {
            int[] continuousIndices = IntStream.range(1, 17).toArray();
            int[] discreteIndices = IntStream.range(17,59).toArray();
        
            double continuousDistance = calculateEuclideanDistance(testSample, trainSample, continuousIndices);
            double discreteDistance = calculateHammingDistance(testSample, trainSample, discreteIndices);
        
            double continuousWeight = 0.99;
            double discreteWeight = 0.01;
        
            return continuousWeight * continuousDistance + discreteWeight * discreteDistance;
        }
        private double calculateEuclideanDistance(String[] testSample, String[] trainSample, int[] indices) {
            double sum = 0;
            for (int index : indices) {
                double testValue = Double.parseDouble(testSample[index]);
                double trainValue = Double.parseDouble(trainSample[index]);
                sum += Math.pow(testValue - trainValue, 2);
            }
            return Math.sqrt(sum);
        }
        
        private double calculateHammingDistance(String[] testSample, String[] trainSample, int[] indices) {
            int differences = 0;
            for (int index : indices) {
                if (!testSample[index].equals(trainSample[index])) {
                    differences++;
                }
            }
            return differences;
        }
    }

    public static class PredBreakReducer extends Reducer<Text, Text, Text, Text> {
        private int truePositive = 0;
        private int falsePositive = 0;
        private int trueNegative = 0;
        private int falseNegative = 0;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] value = val.toString().split(",");
                if (value.length >= 2){
                    double predictedValue = Double.parseDouble(value[0]);
                    double actualValue = Double.parseDouble(value[1]);

                    // 计算混淆矩阵
                    if (predictedValue == 1 && actualValue == 1) {
                        truePositive++;
                    } else if (predictedValue == 1 && actualValue == 0) {
                        falsePositive++;
                    } else if (predictedValue == 0 && actualValue == 1) {
                        falseNegative++;
                    } else {
                        trueNegative++;
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException,InterruptedException{
            // 计算评估指标
            double accuracy = (double) (truePositive + trueNegative) / (truePositive + falsePositive + falseNegative + trueNegative);
            double precision = (double) truePositive / (truePositive + falsePositive);
            double recall = (double) truePositive / (truePositive + falseNegative);
            double f1Score = 2 * (precision * recall) / (precision + recall);

            // 输出结果
            context.write(new Text("Accuracy:"), new Text(String.valueOf(accuracy)));
            context.write(new Text("Precision:"), new Text(String.valueOf(precision)));
            context.write(new Text("Recall:"), new Text(String.valueOf(recall)));
            context.write(new Text("F1-Score:"), new Text(String.valueOf(f1Score)));
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PredBreak");

        job.setJarByClass(PredBreak.class);
        job.setMapperClass(PredBreakMapper.class);
        //job.setCombinerClass(PredBreakReducer.class);
        job.setReducerClass(PredBreakReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("/user/cmh/input/train.csv"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
