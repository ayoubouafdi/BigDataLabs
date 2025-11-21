package edu.supmti.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        
        // classe principale [cite: 145]
        job.setJarByClass (WordCount.class);
        
        // classe qui fait le map [cite: 147]
        job.setMapperClass (TokenizerMapper.class);
        
        // classe qui fait le shuffling et le reduce (IntSumReducer est utilisé comme Combiner pour l'optimisation) [cite: 148]
        job.setCombinerClass (IntSumReducer.class);
        job.setReducerClass (IntSumReducer.class);
        
        // Types de sortie [cite: 149, 150]
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (IntWritable.class);
        
        // spécifier le fichier d'entrée [cite: 152]
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // spécifier le fichier contenant le résultat [cite: 154]
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Lancement du job
        System.exit(job.waitForCompletion (true) ? 0:1);
    }
}