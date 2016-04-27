package com.vientiuno.jchart.graphs;

import org.jfree.ui.RefineryUtilities;


/**
 * Jchart Graphs Application Driver
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {


        LineChart championship = new LineChart("Championship", "data/novaChampionship.txt", "Nova", "data/uncChampionship.txt", "UNC");
        championship.pack();
        RefineryUtilities.centerFrameOnScreen(championship);
        championship.setVisible(true);

        LineChart ff1 = new LineChart("Nova-OU Final Four", "data/novaFF.txt", "Nova", "data/ouFF.txt", "OU");
        ff1.pack();
        RefineryUtilities.centerFrameOnScreen(ff1);
        ff1.setVisible(true);

        LineChart ff2 = new LineChart("Cuse-UNC Final Four", "data/cuseFF.txt", "Syracuse", "data/uncFF.txt", "UNC");
        ff2.pack();
        RefineryUtilities.centerFrameOnScreen(ff2);
        ff2.setVisible(true);

    }
}
