
package HadoopTDG.Chapter02;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


import java.net.URI;

/**
 * Date: 2022/04/05
 * Author: zhangyuyang
 * Description:
 */
public class FileSystemMove {
    public static void main(String[] args) {
        String beginStr = args[0];
        String endStr = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            if (!fs.exists(new Path(endStr))) {
                fs.rename(new Path(beginStr), new Path(endStr));
            }
        }catch (Exception e){
            System.out.println(e);
        }finally {
        }

    }
}
