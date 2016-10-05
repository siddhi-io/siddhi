package org.wso2.siddhi.extension.reorder;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

//import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

import static java.lang.Math.*;
/**
 * Created by vithursa on 8/18/16.
 */
public class ThetaThreshold {
    private long N;
    private static double theta1, theta2, theta3, theta4;
    private double delta;
    private int windowSize;
    private double errorThreshold;
    //---------------------------How to calculate TupleArrivalRate?????-------------------------------------------------
    private double tupleArrivalRate = 10; //refers to the number of tuples arrive within unit time interval.

    //--------------------------need to calculate IdealMean and IdealVariance-------------------------------------------
    private double idealMean;
    private double idealVariance;

    //--------------------------------Calculating Values --------------------------------------------------------------
    private double criticalValue;
    private static double thetaThresholdValue;


    public ThetaThreshold(double errorThreshold, double delta, int windowSize) {
        this.delta = delta;
        this.windowSize =windowSize;
        this.errorThreshold = errorThreshold;
    }


    //------------------------------------Number of tuples in ideal window scope ---------------------------------------
    public long ActualTupleSizeCalculator() {
        N = windowSize * round(tupleArrivalRate);
        return N;
    }

    //-------------------------------------Calculation of mean and variance --------------------------------------------
    public double MeanCalculator(ArrayList listOfEvents) {
        N = listOfEvents.size();
        int mean = 0;
        for (int i = 0; i < listOfEvents.size()/*( listOfEvents.size() > N ? N : listOfEvents.size())*/; i++) {
            mean += (Integer)listOfEvents.get(i);
        }
        idealMean = mean *1.0/ listOfEvents.size();
        return idealMean;
    }

    public double VarianceCalculator(ArrayList listOfEvents) {
        double variance = 0;
        for (int i = 0; i < listOfEvents.size() /*(listOfEvents.size() > N ? N : listOfEvents.size())*/; i++) {
            variance += pow(idealMean - (Integer)listOfEvents.get(i), 2);
        }
        idealVariance = variance*1.0 / listOfEvents.size();
        return idealVariance;
    }


    //------------------------------------Calculation of CriticalValue -------------------------------------------------

    public double CriticalValueCalculator() {
        NormalDistribution ActualDistribution = new NormalDistribution();
        criticalValue = Math.abs(ActualDistribution.inverseCumulativeProbability(delta / 2));
        return criticalValue;
    }

    //-----------------------------------Solving inequality to get ThetaThreshold --------------------------------------

    public double ThetaThresholdCalculator(double criticalValue, double idealMean, double idealVariance) {
        double temp1 = Math.sqrt((pow(idealMean, 2) + pow(idealVariance, 2)) / (N * pow(idealMean, 2)));
        double temp2 = pow(criticalValue, 2) * pow(temp1, 2);
        //System.out.println((pow(idealMean, 2) + pow(idealVariance, 2)));

        double a1, b1, c1, b2, c2;
        a1 = 1 + temp2;
        b1 = (2 * errorThreshold) - 2 - temp2;
        c1 = pow((1 - errorThreshold), 2);
        b2 = -(2 + (2 * errorThreshold) + temp2);
        c2 = pow((1 + errorThreshold), 2);
        //System.out.println(a1+"\t\t\t"+b1+"\t\t\t"+c1+"\t\t\t"+b2+"\t\t\t"+c2);
        double tempSq1 = pow(b1, 2) - (4 * a1 * c1);
        double tempSq2 = pow(b2, 2) - (4 * a1 * c2);

        if (tempSq1 >= 0) {
            theta1 = (-b1 - sqrt(tempSq1)) / (2 * a1);
            theta2 = (-b1 + sqrt(tempSq1)) / (2 * a1);
        }
        else {
            theta1 = theta2 = 0;
        }

        if (tempSq2 >= 0) {
            theta3 = (-b2 - sqrt(tempSq2)) / (2 * a1);
            theta4 = (-b2 + sqrt(tempSq2)) / (2 * a1);
        }
        else {
            theta3 = theta4 = 0;
        }

        if (tempSq1 >= 0 & tempSq2 < 0) {
            thetaThresholdValue = min(theta1, theta2);
        }
        else if (tempSq2 >= 0 & tempSq1 < 0) {
            thetaThresholdValue = min(theta3, theta4);
        }
        else if (tempSq1 < 0 & tempSq2 < 0) {
            thetaThresholdValue = 0;
        }
        else {
            thetaThresholdValue = min(min(theta1, theta2), min(theta3, theta4));
        }
        return thetaThresholdValue;
        //System.out.println(theta1+"\t\t\t"+theta2+"\t\t\t"+theta3+"\t\t\t"+theta4);
    }

   /* public static void main(String[] args) {
        try {
            my = new PrintStream(new FileOutputStream("MyOutFile.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        /*for (int i = 0; i < 1000; i++) {
            windowCoverageArray.add(random.nextDouble());
        }*/


        /*for(int i=0; i < 10; i++){
            for(int j=0; j < 10; j++){
                for(int k=0; k < 10; k++){
                    org.wso2.siddhi.extension.reorder.ThetaThreshold obj1 = new ThetaThreshold((0.01 * i),(0.01*j),k);
                    obj1.ActualTupleSizeCalculator(k);
                    ans = obj1.ThetaThresholdCalculator(obj1.CriticalValueCalculator((0.01 * i)),34.3,2.5,(0.01 * j));
                    thresholdArray.add(ans);
                }
            }

        }*/

    //--------------------------------------Error and deltaAlpha calculation----------------------------------------
        /*for (int j = 0; j < windowCoverageArray.size(); j++) {
            double Error = thresholdArray.get(j)-windowCoverageArray.get(j);
            errorArray.add(Error);
        }
        deltaAlpha.add((kP * errorArray.get(0)) + (kD * (errorArray.get(0) - 0)));
        for (int k = 1; k < windowCoverageArray.size(); k++) {
            deltaAlpha.add((kP * errorArray.get(k)) + (kD * (errorArray.get(k) - errorArray.get(k - 1))));
        }
        alpha.add(0,1+deltaAlpha.get(0));

        for(int j=1;j<deltaAlpha.size();j++){
            alpha.add(alpha.get(j-1)+deltaAlpha.get(j));
        }

        for(int k=1;k<1000;k++) {
            if ((errorArray.get(k) - errorArray.get(k - 1) >= 0) & (deltaAlpha.get(k) - deltaAlpha.get(k - 1) >= 0)) {
                my.println("\t\t" + errorArray.get(k) + "\t\t\t" + deltaAlpha.get(k));
            }
            else if ((errorArray.get(k) - errorArray.get(k - 1) < 0) & (deltaAlpha.get(k) - deltaAlpha.get(k - 1) < 0)) {
                my.println("\t\t" +errorArray.get(k) + "\t\t\t" + deltaAlpha.get(k));
            }
        }


        my.close();

    }*/


}