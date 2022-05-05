
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Date: 2022/04/19
 * Author: zhangyuyang
 * Description:
 */
public class Task2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "A1_Task2");
        job.setJarByClass(Task2.class);
        job.setMapperClass(Task2_Mapper.class);
        job.setReducerClass(Max_Min_Sum_Reducer.class);

        // default output key and value type for Map and Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Specific Map value output type
        job.setMapOutputValueClass(IntWritable.class);

        // the location of input and output file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // submit and exit job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class Task2_Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text state = new Text(); // output key
        private IntWritable rainfall = new IntWritable(); // output value

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ","); // separate by line
            while (itr.hasMoreTokens()) {
                state.set(itr.nextToken()); // state
                String cities = itr.nextToken();
                rainfall.set(Integer.parseInt(itr.nextToken().trim()));
                context.write(state, rainfall);
            } // end while
        } // end map()
    } // end Task2_Mapper


    public static class Max_Min_Sum_Reducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            for (IntWritable val : values) {
                if (val.get() > max)
                    max = val.get(); // get max
                if (val.get() < min)
                    min = val.get(); // get min
                sum += val.get(); // get total
            } // end for

            // format output value
            StringBuilder result_s = new StringBuilder();
            result_s.append(sum);
            result_s.append(" ");
            result_s.append(max);
            result_s.append(" ");
            result_s.append(min);

            result.set(result_s.toString()); // set output value
            context.write(key, result);
        } // end reduce()
    } // end Max_Min_Sum_Reducer
}
