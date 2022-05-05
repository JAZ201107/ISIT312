
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
public class Task3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "A1_Task3");
        job.setJarByClass(Task3.class);
        job.setMapperClass(Task3_Mapper.class);
        job.setReducerClass(WordLengthReducer.class);

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


    public static class Task3_Mapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text length_type = new Text(); // output key
        private final static IntWritable one = new IntWritable(1); // output value

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                int length = itr.nextToken().length(); // get the length of word
                length_type.set(lengthString(length));  // set output key
                context.write(length_type, one);
            } // end while
        } // end map()

        // get the type of word with length
        public String lengthString(int length) {
            String result = "";
            if (1 <= length && length <= 3) result = "X short";
            else if (4 <= length && length <= 5) result = "short";
            else if (6 <= length && length <= 8) result = "medium";
            else if (9 <= length && length <= 12) result = "long";
            else if (13 <= length && length <= 15) result = "X long";
            else if (16 <= length) result = "XX long";
            return result;
        } // end lengthString()

    } // end Task3_Mapper


    public static class WordLengthReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text count = new Text(); // out put value format like: xx words

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // get the total number of words
            }
            String count_string = String.format("%d\twords", sum); // format the output
            count.set(count_string); // output value
            context.write(key, count);
        } // end reduce()
    } // end WordLengthReducer
}
