package com.vientiuno.jchart.graphs;

import org.jfree.ui.RefineryUtilities;


/**
 * Jchart Graphs Application Driver
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {


        LineChart championship = new LineChart("Championship", "novaChampionship.txt", "Nova", "uncChampionship.txt", "UNC");
        championship.pack();
        RefineryUtilities.centerFrameOnScreen(championship);
        championship.setVisible(true);

        LineChart ff1 = new LineChart("Nova-OU Final Four", "novaFF.txt", "Nova", "ouFF.txt", "OU");
        ff1.pack();
        RefineryUtilities.centerFrameOnScreen(ff1);
        ff1.setVisible(true);

        LineChart ff2 = new LineChart("Cuse-UNC Final Four", "cuseFF.txt", "Syracuse", "uncFF.txt", "UNC");
        ff2.pack();
        RefineryUtilities.centerFrameOnScreen(ff2);
        ff2.setVisible(true);

    }
}
