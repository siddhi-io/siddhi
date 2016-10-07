package org.wso2.siddhi.extension.reorder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by vithursa on 10/3/16.
 */
public class OOOccurence {
    private String filepath;
    private int events;
    private long lateArrival;
    private long tCurr;
    private long delay;
    private long cumulativeDelay=0;
    private int eventCounter =0;
    private int count=0;
    private int count2=0;
    private int count3=0;
    private int count4=0;
    private int batchSize=10000;
    private ArrayList<Long> eventts = new ArrayList<Long>();
    private LinkedHashSet hashset = new LinkedHashSet();
    private boolean flag = false;
    public OOOccurence(String filePath, int events){
        this.filepath = filePath;
        this.events = events;
    }
    public void counter(){
        DataLoader dataLoader = new DataLoader(filepath,events);
        dataLoader.runSingleStream();
        LinkedBlockingQueue eventBuffer = dataLoader.getEventBuffer();
        Iterator itr = eventBuffer.iterator();
        while(itr.hasNext()){
            eventCounter+=1;
            Object[] a = (Object[]) itr.next();
            eventts.add((Long)a[1]);
            if((Long)a[1]>tCurr){
                tCurr = (Long)a[1];
            }
        }

        for(int i=1;i<eventts.size();i++){
            if(eventts.get(i-1)>tCurr){
                tCurr = eventts.get(i-1);
            }
            if(eventts.get(i-1)>eventts.get(i)){
                lateArrival = eventts.get(i);
                delay = tCurr - lateArrival;
                //System.out.println(delay);
                count +=1;
                cumulativeDelay += delay;
                /*if(count2>batchSize){
                    System.out.println(count4);
                    count3+=1;
                    System.out.println(cumulativeDelay/batchSize+"\t\t"+count3);
                    count2=0;
                    cumulativeDelay =0;
                }*/

            }
        }
        System.out.println(count);
        /*System.out.println("Cumulative Delay:\t"+cumulativeDelay);
        System.out.println("Events:\t\t\t\t"+eventCounter);*/
    }


    public static void main(String[] args){
        OOOccurence obj = new OOOccurence("/home/vithursa/Desktop/Reorder_Result/AKSlackCurr",500000);
        obj.counter();
    }
}
