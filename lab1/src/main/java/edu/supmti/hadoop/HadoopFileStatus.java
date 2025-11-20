package edu.supmti.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileStatus {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: HadoopFileStatus <hdfs-input-dir> <filename> <newname>");
            System.exit(1);
        }
        String directory = args[0];
        String filename = args[1];
        String newName = args[2];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(directory + "/" + filename);
        if (!fs.exists(filePath)) {
            System.out.println("File does not exist: " + filePath);
            fs.close();
            return;
        }

        FileStatus status = fs.getFileStatus(filePath);

        System.out.println("File Name: " + filename);
        System.out.println("Size: " + status.getLen() + " bytes");
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Permission: " + status.getPermission());
        System.out.println("Replication: " + status.getReplication());
        System.out.println("Block Size: " + status.getBlockSize());

        Path newFilePath = new Path(directory + "/" + newName);
        fs.rename(filePath, newFilePath);
        System.out.println("File renamed to: " + newName);
        fs.close();
    }
}
