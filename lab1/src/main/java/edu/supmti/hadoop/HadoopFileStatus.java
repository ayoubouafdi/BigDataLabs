package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            if (args.length < 3) {
                System.out.println("Usage: <hdfs_path> <filename> <new_filename>");
                System.exit(1);
            }

            Path filePath = new Path(args[0] + "/" + args[1]);
            if (!fs.exists(filePath)) {
                System.out.println("File does not exist: " + filePath);
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filePath);
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("Size: " + status.getLen() + " bytes");
            System.out.println("Owner: " + status.getOwner());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());

            Path newPath = new Path(args[0] + "/" + args[2]);
            fs.rename(filePath, newPath);
            System.out.println("File renamed to: " + args[2]);

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
