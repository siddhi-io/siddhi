package org.wso2.siddhi.extension.reorder;

import org.wso2.siddhi.query.api.expression.condition.In;
import org.wso2.siddhi.query.api.expression.constant.DoubleConstant;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Math.min;

/**
 * Created by vithursa on 9/11/16.
 */
public class Main {
    //private static LinkedBlockingQueue eventBuffer =new LinkedBlockingQueue();
    private static ArrayList<Long> timeStamps = new ArrayList<Long>();
    private static ArrayList eventTimeStamps;
    private static ArrayList velocity = new ArrayList();
    private static ArrayList subList = new ArrayList();
    private static boolean indicator = false;
    private static int NANOSECOND = 1000000000;

    public static void main(String[] args) throws InterruptedException {
        int batchSize = 10000;
        int counter1 = 1;
        ArrayList differenceOfError = new ArrayList();
        ArrayList temp = new ArrayList();
        ArrayList thetaThresholdList = new ArrayList();
        LinkedHashSet<Integer> correctVelocity = new LinkedHashSet<Integer>();
        ArrayList errorList = new ArrayList();
        ArrayList deltaAlpha = new ArrayList();
        ArrayList alpha = new ArrayList();

        DataLoader inputData = new DataLoader("/home/vithursa/Downloads/my.txt",500000);
        inputData.runSingleStream();
        LinkedBlockingQueue events = inputData.getEventBuffer();
        ThetaThreshold obj2 = new ThetaThreshold(0.2,0.5,10000);

        for(int i=0;i<500000;i++){
            if(counter1 > batchSize){
                Iterator<Integer> itr = velocity.iterator();
                while(itr.hasNext()){
                    correctVelocity.add(itr.next());
                }
                System.out.println(correctVelocity);
                temp.addAll(correctVelocity);
                double criticalValue = obj2.CriticalValueCalculator();
                obj2.ThetaThresholdCalculator(criticalValue,obj2.MeanCalculator(temp),obj2.VarianceCalculator(temp));
                velocity = new ArrayList<Long>();
                correctVelocity = new LinkedHashSet<Integer>();
                counter1 = 1;
            }

            Object[] b = (Object[]) events.take();
            velocity.add((int) (((Integer) b[5])*1.0/1000000));
            counter1+=1;
        }

//----------------------------------------------------------------------------------------------------------------------
        long starttime = System.currentTimeMillis();
        DataLoader obj1 = new DataLoader("/home/vithursa/Desktop/OOEvents",500000);
        obj1.runSingleStream();
        LinkedBlockingQueue eventBuffer = obj1.getEventBuffer();

        int counter=1;
        int counter2 = 0;
        Runtime obj5 = new Runtime();
        ArrayList windowCoverageList = new ArrayList();
        LinkedHashSet<Long> eventTimeStamps = new LinkedHashSet<Long>();

        for(int i=0;i<500000;i++){
            if(counter > batchSize){
                Iterator<Long> itr = timeStamps.iterator();
                while(itr.hasNext()){
                    long data = itr.next()/NANOSECOND;
                    eventTimeStamps.add(data);
                }

                ArrayList<Long> itemsList = obj5.CurentTimeStamp(eventTimeStamps);
                obj5.WindowCoverageCalculator(eventTimeStamps, itemsList);

                timeStamps = new ArrayList<Long>();
                eventTimeStamps = new LinkedHashSet<Long>();
                counter = 1;
                counter2+=1;
                //System.out.println("counter2: "+counter2);
            }

            Object[] a = (Object[]) eventBuffer.take();
            timeStamps.add((Long) a[1]);
            counter+=1;
        }

        for(int i =0;i<windowCoverageList.size();i++){
            Double error = (Double)thetaThresholdList.get(i)-(Double) windowCoverageList.get(i);
            errorList.add(error);
        }

        differenceOfError.add(errorList.get(0));
        for(int h=1;h<errorList.size();h++){
            double difference = (Double)errorList.get(h)-(Double) errorList.get(h-1);
            differenceOfError.add(difference);
        }

//---------------------------Calculating derivative of error -----------------------------------------------------------
        for(int k=0;k<errorList.size();k++){
            deltaAlpha.add(0.1*(Double) errorList.get(k)+0.2*(Double)differenceOfError.get(k));
        }
        alpha.add(1.0);

//--------------------------Calculation of alpha -----------------------------------------------------------------------
        for(int j=0;j<deltaAlpha.size();j++){
            alpha.add((Double)deltaAlpha.get(j)+(Double) alpha.get(j));
        }
        for(int d=0;d<errorList.size();d++){
            System.out.println(alpha.get(d));
        }

        long endtime = System.currentTimeMillis();
        System.out.println(endtime-starttime);

    }
}