package com.vientiuno.popular.words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Most Popular Words per Minute Application Driver
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {

        String inputPath = args[0];
        if(inputPath.charAt(inputPath.length() - 1) != '/') inputPath += "/";
        String outPath = args[1];
        if(outPath.charAt(outPath.length()-1) != '/') outPath += "/";

        /* Processing for final four game 1 */
        Configuration ff1Conf = new Configuration();
        ff1Conf.set("start_time", "Sat Apr 02 18:45:00 MDT 2016");
        ff1Conf.set("end_time", "Sat Apr 02 20:50:00 MDT 2016");
        Job ff1 = Job.getInstance(ff1Conf, "main");

        ff1.setJobName("Final Four Game 1");

        ff1.setJarByClass(App.class);

        ff1.setOutputKeyClass(Text.class);
        ff1.setOutputValueClass(IntWritable.class);

        ff1.setMapperClass(WordCountMap.class);
        ff1.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(ff1, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(ff1, new Path(outPath + "finalFourOne"));

        ff1.waitForCompletion(true);

        /* Processing for final four game 2 */
        Configuration ff2Conf = new Configuration();
        ff2Conf.set("start_time", "Sat Apr 02 16:05:00 MDT 2016");
        ff2Conf.set("end_time", "Sat Apr 02 18:10:00 MDT 2016");

        Job ff2 = Job.getInstance(ff2Conf, "main");

        ff2.setJobName("Villanova Final Four");

        ff2.setJarByClass(App.class);

        ff2.setOutputKeyClass(Text.class);
        ff2.setOutputValueClass(IntWritable.class);

        ff2.setMapperClass(WordCountMap.class);
        ff2.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(ff2, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(ff2, new Path(outPath + "finalFour2"));

        ff2.waitForCompletion(true);


        /* Processing for championship game */
        Configuration champConf = new Configuration();
        champConf.set("start_time", "Mon Apr 04 19:15:00 MDT 2016");
        champConf.set("end_time", "Mon Apr 04 21:30:00 MDT 2016");
        Job championship = Job.getInstance(champConf, "main");

        championship.setJobName("Championship");

        championship.setJarByClass(App.class);

        championship.setOutputKeyClass(Text.class);
        championship.setOutputValueClass(IntWritable.class);

        championship.setMapperClass(WordCountMap.class);
        championship.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(championship, new Path(inputPath + "championship.txt"));
        FileOutputFormat.setOutputPath(championship, new Path(outPath + "championship"));

    }
}
