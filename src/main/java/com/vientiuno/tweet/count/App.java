package com.vientiuno.tweet.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jfree.ui.RefineryUtilities;


/**
 * Tweets/Minute Application Driver
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {


        LineChart demo = new LineChart("Villanova Championship");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);
        /*
        String inputPath = args[0];
        if(inputPath.charAt(inputPath.length() - 1) != '/') inputPath += "/";
        String outPath = args[1];
        if(outPath.charAt(outPath.length()-1) != '/') outPath += "/";

        */
        /* Processing for final four games
        Configuration uncFFConf = new Configuration();
        uncFFConf.set("start_time", "Sat Apr 02 18:45:00 MDT 2016");
        uncFFConf.set("end_time", "Sat Apr 02 20:50:00 MDT 2016");
        uncFFConf.set("keywords", "TarHeelNation, Heels, UNC, UNC_Basketball, Carolina, Paige, Berry, Brice");
        Job uncFF = Job.getInstance(uncFFConf, "main");

        uncFF.setJobName("UNC Final Four");

        uncFF.setJarByClass(App.class);

        uncFF.setOutputKeyClass(Text.class);
        uncFF.setOutputValueClass(IntWritable.class);

        uncFF.setMapperClass(Map.class);
        uncFF.setCombinerClass(Reduce.class);
        uncFF.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(uncFF, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(uncFF, new Path(outPath + "uncFinalFour"));

        uncFF.waitForCompletion(true);

        Configuration cuseFFConf = new Configuration();
        cuseFFConf.set("start_time", "Sat Apr 02 18:45:00 MDT 2016");
        cuseFFConf.set("end_time", "Sat Apr 02 20:50:00 MDT 2016");
        cuseFFConf.set("keywords", "Cuse, Syracuse, Orange, Gbinije, Cooney, Malachi");
        Job cuseFF = Job.getInstance(cuseFFConf, "main");

        cuseFF.setJobName("Syracuse Final Four");

        cuseFF.setJarByClass(App.class);

        cuseFF.setOutputKeyClass(Text.class);
        cuseFF.setOutputValueClass(IntWritable.class);

        cuseFF.setMapperClass(Map.class);
        cuseFF.setCombinerClass(Reduce.class);
        cuseFF.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(cuseFF, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(cuseFF, new Path(outPath + "cuseFinalFour"));

        cuseFF.waitForCompletion(true);


        Configuration novaFFConf = new Configuration();
        novaFFConf.set("start_time", "Sat Apr 02 16:05:00 MDT 2016");
        novaFFConf.set("end_time", "Sat Apr 02 18:10:00 MDT 2016");
        novaFFConf.set("keywords", "NovaMBB, Villanova, Nova, Arcidiacono, Jenkins, Ochefu, Wildcats, Hart");

        Job novaFF = Job.getInstance(novaFFConf, "main");

        novaFF.setJobName("Villanova Final Four");

        novaFF.setJarByClass(App.class);

        novaFF.setOutputKeyClass(Text.class);
        novaFF.setOutputValueClass(IntWritable.class);

        novaFF.setMapperClass(Map.class);
        novaFF.setCombinerClass(Reduce.class);
        novaFF.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(novaFF, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(novaFF, new Path(outPath + "novaFinalFour"));

        novaFF.waitForCompletion(true);


        Configuration ouFFConf = new Configuration();
        ouFFConf.set("start_time", "Sat Apr 02 16:05:00 MDT 2016");
        ouFFConf.set("end_time", "Sat Apr 02 18:10:00 MDT 2016");
        ouFFConf.set("keywords", "OU_MBBall, Sooners, Boomer, #OU, Sooner, Oklahoma, Buddy, Hield, Spangler");

        Job ouFF = Job.getInstance(ouFFConf, "main");

        ouFF.setJobName("Oklahoma Final Four");

        ouFF.setJarByClass(App.class);

        ouFF.setOutputKeyClass(Text.class);
        ouFF.setOutputValueClass(IntWritable.class);

        ouFF.setMapperClass(Map.class);
        ouFF.setCombinerClass(Reduce.class);
        ouFF.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(ouFF, new Path(inputPath + "final_four.txt"));
        FileOutputFormat.setOutputPath(ouFF, new Path(outPath + "ouFinalFour"));

        ouFF.waitForCompletion(true);

        */

        /* Processing for championship game
        Configuration uncChampionshipConf = new Configuration();
        uncChampionshipConf.set("start_time", "Mon Apr 04 19:15:00 MDT 2016");
        uncChampionshipConf.set("end_time", "Mon Apr 04 21:30:00 MDT 2016");
        uncChampionshipConf.set("keywords", "Heel, Heels, UNC, Carolina, Paige, Berry, Brice");
        Job uncChampionship = Job.getInstance(uncChampionshipConf, "main");

        uncChampionship.setJobName("UNC Championship");

        uncChampionship.setJarByClass(App.class);

        uncChampionship.setOutputKeyClass(Text.class);
        uncChampionship.setOutputValueClass(IntWritable.class);

        uncChampionship.setMapperClass(Map.class);
        uncChampionship.setCombinerClass(Reduce.class);
        uncChampionship.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(uncChampionship, new Path(inputPath + "championship.txt"));
        FileOutputFormat.setOutputPath(uncChampionship, new Path(outPath + "uncChampionship"));

        uncChampionship.waitForCompletion(true);

        Configuration novaChampionshipConf = new Configuration();
        novaChampionshipConf.set("start_time", "Mon Apr 04 19:15:00 MDT 2016");
        novaChampionshipConf.set("end_time", "Mon Apr 04 21:30:00 MDT 2016");
        novaChampionshipConf.set("keywords", "NovaMBB, Villanova, Nova, Jenkins, Arcidiacono, Wildcats, Ochefu, Hart");

        Job novaChampionship = Job.getInstance(novaChampionshipConf, "main");

        novaChampionship.setJobName("Villanova Championship");

        novaChampionship.setJarByClass(App.class);

        novaChampionship.setOutputKeyClass(Text.class);
        novaChampionship.setOutputValueClass(IntWritable.class);

        novaChampionship.setMapperClass(Map.class);
        novaChampionship.setCombinerClass(Reduce.class);
        novaChampionship.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(novaChampionship, new Path(inputPath + "championship.txt"));
        FileOutputFormat.setOutputPath(novaChampionship, new Path(outPath + "novaChampionship"));

        novaChampionship.waitForCompletion(true);

        */
    }
}
