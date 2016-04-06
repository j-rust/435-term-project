package com.vientiuno.term.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Application Driver
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {

        Configuration uncChampionshipConf = new Configuration();
        uncChampionshipConf.set("start_time", "Mon Apr 04 19:15:00 MDT 2016");
        uncChampionshipConf.set("end_time", "Mon Apr 04 21:30:00 MDT 2016");
        uncChampionshipConf.set("keywords", "HeelsLockIn, TarHeelNation, TarHeels, Tar Heels, GoHeels, UNC, UNC_Basketball, " +
                "HeelsInHouston, North Carolina, Go Heels");
        Job uncChampionship = Job.getInstance(uncChampionshipConf, "main");

        uncChampionship.setJobName("UNC Championship");

        uncChampionship.setJarByClass(App.class);

        uncChampionship.setOutputKeyClass(Text.class);
        uncChampionship.setOutputValueClass(IntWritable.class);

        uncChampionship.setMapperClass(Map.class);
        uncChampionship.setCombinerClass(Reduce.class);
        uncChampionship.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(uncChampionship, new Path(args[0]));
        FileOutputFormat.setOutputPath(uncChampionship, new Path(args[1]));

        uncChampionship.waitForCompletion(true);

    }
}
