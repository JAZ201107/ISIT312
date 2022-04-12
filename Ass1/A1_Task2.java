package HadoopTDG.Chapter02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Date: 2022/04/12
 * Author: zhangyuyang
 * Description:
 */
public class A1_Task2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RanFall");
        job.setJarByClass(MinMax.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" ");
            String state_name = fields[0];
            String rainfall = fields[2];

            context.write(new Text(state_name), new Text(rainfall));
        } // end map
    } // end Map

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            for (IntWritable val : values) {
                if (val.get() > max) max = val.get();
                if (val.get() < min) min = val.get();
                sum += val.get();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(sum);
            sb.append("\t");
            sb.append(max);
            sb.append("\t");
            sb.append(min);

            context.write(key, new Text(sb.toString()));
        }
    } // end Reduce
}
