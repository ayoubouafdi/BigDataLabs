package edu.supmti.hadoop;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage:");
            System.out.println(" 1) WriteHDFS <hdfs-file-path> <text>");
            System.out.println(" 2) WriteHDFS <hdfs-file-path> --local <local-file>");
            System.exit(1);
        }

        String hdfsDest = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path dest = new Path(hdfsDest);

        if (args.length == 2) {
            String text = args[1];
            FSDataOutputStream out = fs.create(dest, true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            bw.write(text);
            bw.newLine();
            bw.close();
            out.close();
            System.out.println("Written text into " + hdfsDest);
        } else if ("--local".equals(args[1]) && args.length == 3) {
            String localPath = args[2];
            FSDataOutputStream out = fs.create(dest, true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            Files.lines(Paths.get(localPath)).forEach(line -> {
                try {
                    bw.write(line);
                    bw.newLine();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            bw.close();
            out.close();
            System.out.println("Copied local file " + localPath + " to " + hdfsDest);
        } else {
            System.out.println("Invalid arguments. See usage.");
        }
        fs.close();
    }
}
