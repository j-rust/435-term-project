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

        String outPath = args[1];
        if(outPath.charAt(outPath.length()-1) != '/') outPath += "/";

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
        FileOutputFormat.setOutputPath(uncChampionship, new Path(outPath + "uncChampionship"));

        uncChampionship.waitForCompletion(true);

        Configuration novaChampionshipConf = new Configuration();
        novaChampionshipConf.set("start_time", "Mon Apr 04 19:15:00 MDT 2016");
        novaChampionshipConf.set("end_time", "Mon Apr 04 21:30:00 MDT 2016");
        novaChampionshipConf.set("keywords", "NovaMBB, Villanova, GoNova, NovaNation, #Nova, LetsMarchNova");

        Job novaChampionship = Job.getInstance(novaChampionshipConf, "main");

        novaChampionship.setJobName("Villanova Championship");

        novaChampionship.setJarByClass(App.class);

        novaChampionship.setOutputKeyClass(Text.class);
        novaChampionship.setOutputValueClass(IntWritable.class);

        novaChampionship.setMapperClass(Map.class);
        novaChampionship.setCombinerClass(Reduce.class);
        novaChampionship.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(novaChampionship, new Path(args[0]));
        FileOutputFormat.setOutputPath(novaChampionship, new Path(outPath + "villanova"));

        novaChampionship.waitForCompletion(true);


    }
}
