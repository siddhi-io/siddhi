package org.wso2.siddhi.extension.reorder;

import org.wso2.siddhi.core.table.holder.ListEventHolder;
import org.wso2.siddhi.query.api.expression.condition.In;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Math.abs;
import static java.lang.Math.round;

/**
 * Created by vithursa on 9/2/16.
 */
public class Runtime{
    private ArrayList eventStream = new ArrayList();
    private ArrayList timeStamps = new ArrayList();
    private int numerator;
    private int denominator;
    private double windowCoverage;
    private static int MILISECOND = 1000;
    private static int MICROSECOND = 1000000;
    private int NANOSECOND = 1000000000;
    private int TIME_UNIT=MILISECOND;
    private int TIME_WINDOW_LEN= 20;
    //private long windowSize = TIME_WINDOW_LEN;
    private long windowSize = 10000l;
    private long currentTimeStamp;
    private long q =1;
    private LinkedBlockingQueue<Object> eventBuffer;
    private DataLoader dataLoader;
    private HashMap<Long, ArrayList<Long>> timeStampMap = new HashMap<Long, ArrayList<Long>>();
    private long lowerIndex;


    //---------------------------------------------------Get current timestamp value ---------------------------------------
    public ArrayList CurentTimeStamp(LinkedHashSet<Long> timeStamps){
        Iterator<Long> itr = timeStamps.iterator();
        int counter = 1;
        //int WINDOW_SIZE = 1000;
        long item = 0;
        long largestItem = 0;
        ArrayList<Long> list = new ArrayList<Long>();

        while(itr.hasNext()){
            item = itr.next();

            if(item > largestItem){
                largestItem = item;
            }

            if(counter == timeStamps.size()){
                list.add(largestItem);
                counter = 1;
                largestItem = 0;
            }

            counter+=1;
        }
        return list;
    }

    //----------------------------------------------------Calculating Window Coverage --------------------------------------
    public double WindowCoverageCalculator(LinkedHashSet<Long> eventTimeStamps , ArrayList currentTimeStampList) {
        long count = 1;
        timeStamps.addAll(eventTimeStamps);
        Iterator itr = eventTimeStamps.iterator();
        for(int a =0;a<currentTimeStampList.size();a++) {
            currentTimeStamp = (Long) currentTimeStampList.get(a);
            long edgeValue = (currentTimeStamp - q - windowSize);
            long d = (Long) itr.next();
            long distance = abs(d - edgeValue);
            while (itr.hasNext()) {
                //for (int l = 0; l < timeStamps.size(); l++) {
                long c = (Long) itr.next();
                long cdistance = abs(c - edgeValue);
                if ((cdistance < distance) & cdistance != 0) {
                    distance = cdistance;
                    lowerIndex = (long) count;
                }
                count += 1;
            }
            count = 1;
            itr = eventTimeStamps.iterator();
            try {
                for (long i = edgeValue + 1; i <= currentTimeStamp - q; i++) {
                    if (eventTimeStamps.contains(i)) {
                        int z = timeStamps.indexOf(i);
                        int y = timeStamps.indexOf(currentTimeStamp);
                        if (z <= (y - q) && z >= lowerIndex) {
                            numerator += 1;
                        }
                        denominator += 1;
                    }
                }
                windowCoverage = numerator * 1.0 / denominator;
                //windowCoverageList.add(windowCoverage);
            } catch (Exception e) {
                System.out.println(e);
            }
            numerator = 0;
            denominator = 0;
            lowerIndex = 0;
            timeStamps = new ArrayList();
        }
        return windowCoverage;
    }
}
