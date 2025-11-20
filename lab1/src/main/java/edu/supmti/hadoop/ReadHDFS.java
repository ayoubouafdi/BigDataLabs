package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: ReadHDFS <hdfs-file-path>");
            System.exit(1);
        }
        String hdfsFile = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(hdfsFile);
        if (!fs.exists(path)) {
            System.out.println("File does not exist: " + hdfsFile);
            fs.close();
            return;
        }

        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
        in.close();
        fs.close();
    }
}
