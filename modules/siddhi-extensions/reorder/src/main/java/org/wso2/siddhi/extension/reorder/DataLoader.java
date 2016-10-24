package org.wso2.siddhi.extension.reorder;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by vithursa on 9/11/16.
 */
public class DataLoader implements DataLoaderThread{
    private String fileName;
    private static Splitter splitter = Splitter.on(',');
    private LinkedBlockingQueue<Object> eventBufferList;
    private BufferedReader br;
    private int count;
    private long eventLimit;//This many events will be read from the stream data set
    //private int sensorIDField = -1;
    //private boolean calibrated;
    //private String FILE_PATH = "sensors.xml";
    //private HashMap<Integer, Integer> sensorMap;

    public DataLoader(String fileName, long eventCount){
        this.fileName = fileName;
        eventBufferList = new LinkedBlockingQueue<Object>();
        this.eventLimit = eventCount;
    }

    public void runSingleStream(){
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String sid = dataStrIterator.next(); //sensor id
                String ts = dataStrIterator.next(); //Timestamp in pico seconds
                String x = dataStrIterator.next();
                String y = dataStrIterator.next();
                String z = dataStrIterator.next();
                String v_abs = dataStrIterator.next();
                String a_abs = dataStrIterator.next();
                String vx = dataStrIterator.next();
                String vy = dataStrIterator.next();
                String vz = dataStrIterator.next();
                String ax = dataStrIterator.next();
                String ay = dataStrIterator.next();
                String az = dataStrIterator.next();

                Object[] eventData = null;

                try{
                    eventData = new Object[]{
                            Integer.parseInt(sid),
                            Long.parseLong(ts),
                            Integer.parseInt(x), //This can be represented by two bytes
                            Integer.parseInt(y),
                            Integer.parseInt(z),
                            Integer.parseInt(v_abs),
                            Integer.parseInt(a_abs),
                            Integer.parseInt(vx),
                            Integer.parseInt(vy),
                            Integer.parseInt(vz),
                            Integer.parseInt(ax),
                            Integer.parseInt(ay),
                            Integer.parseInt(az),
                            -1l
                    };
                }catch(NumberFormatException e){
                    //e.printStackTrace();
                    //If we find a discrepancy in converting data, then we have to discard that
                    //particular event.
                    line = br.readLine();
                    continue;
                }

                //We keep on accumulating data on to the event queue.
                //This will get blocked if the space required is not available.
                eventBufferList.put(eventData);
                line = br.readLine();
                count++;

                if(count >= eventLimit){
                    break;
                }
            }
            //System.out.println("Total amount of events read : " + count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        //System.out.println("Now exiting from data loader thread.");
    }

    @Override
    public void run() {
    }

    @Override
    public LinkedBlockingQueue<Object> getEventBuffer() {
        return eventBufferList;
    }
}
