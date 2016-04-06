package com.vientiuno.term.project;

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

    public LineChart(String title) {
        super(title);
        final XYDataset dataset = createDataset();
        final JFreeChart chart = createChart(dataset);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);

    }

    private XYDataset createDataset() {

        final XYSeries data = new XYSeries("Nova");
        XYSeriesCollection set = new XYSeriesCollection();
        try {
            Scanner scan = new Scanner(new File("part-r-00000"));
            int minute = 0;
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                String[] split = line.split("\t");
                data.add(minute, Integer.parseInt(split[1]));
                minute++;
            }
            set.addSeries(data);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return set;
    }

    private JFreeChart createChart(final XYDataset dataset) {
        JFreeChart chart =  ChartFactory.createXYLineChart("Villanova Championship", "Minute", "Tweets",
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
        plot.setRenderer(renderer);
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        return chart;
    }
}
