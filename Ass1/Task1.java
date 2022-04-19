
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Date: 2022/04/19
 * Author: zhangyuyang
 * Description:
 */

public class Task1 {
    public static void main(String[] args) throws IOException {
        // get file name
        String in_file = args[0];
        String out_file = args[1];

        // read configuration file
        Configuration conf = new Configuration();
        // get an instance of HDFS
        FileSystem fs = FileSystem.get(URI.create(in_file), conf);

        // Path object and inout file stream to read file in
        Path in_path = new Path(in_file);
        FSDataInputStream in_fs = fs.open(in_path);

        // Path object and inout file stream to read file out
        Path out_path = new Path(out_file);
        FSDataOutputStream out_fs = fs.create(out_path);

        // write file out
        byte[] buffer = new byte[256];
        int bytesRead = 0;
        while((bytesRead = in_fs.read(buffer)) > 0) {
            out_fs.write(buffer,0,bytesRead);
        }

//        IOUtils.copyBytes(in_fs, out_fs, 4096, true);
        // close file
        in_fs.close();
        out_fs.close();

        // delete the original file
        fs.delete(in_path, true);
    }
}
