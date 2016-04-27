package com.vientiuno.jchart.graphs;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.chart.axis.NumberAxis;


import java.awt.*;
import java.io.File;
import java.util.Scanner;

/**
 * Created by Jonathan Rust on 4/6/16.
 */
public class LineChart extends ApplicationFrame {

    public LineChart(String title, String file1, String title1, String file2, String title2) {
        super(title);
        final XYDataset dataset = createDataset(file1, title1, file2, title2);
        final JFreeChart chart = createChart(dataset, title);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);

    }

    private XYDataset createDataset(String file1, String title1, String file2, String title2) {

        final XYSeries data1 = new XYSeries(title1);
        XYSeriesCollection set = new XYSeriesCollection();
        try {
            Scanner scan = new Scanner(new File(file1));
            int minute = 0;
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                String[] split = line.split("\t");
                data1.add(minute, Integer.parseInt(split[1]));
                minute++;
            }
            scan.close();
            set.addSeries(data1);

            final XYSeries data2 = new XYSeries(title2);
            Scanner scan2 = new Scanner(new File(file2));
            minute = 0;
            while (scan2.hasNextLine()) {
                String line = scan2.nextLine();
                String[] split = line.split("\t");
                data2.add(minute, Integer.parseInt(split[1]));
                minute++;
            }
            scan2.close();
            set.addSeries(data2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return set;
    }

    private JFreeChart createChart(final XYDataset dataset, String title) {
        JFreeChart chart =  ChartFactory.createXYLineChart(title, "Minute", "Tweets",
                dataset, PlotOrientation.VERTICAL, true, true, false);
        chart.setBackgroundPaint(Color.white);
        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.lightGray);
        //    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesLinesVisible(0, true);
        renderer.setSeriesShapesVisible(0, false);
        renderer.setSeriesShapesFilled(0, false);
        renderer.setSeriesLinesVisible(1, true);
        renderer.setSeriesShapesVisible(1, false);
        renderer.setSeriesShapesFilled(1, false);
        plot.setRenderer(renderer);
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        return chart;
    }
}
