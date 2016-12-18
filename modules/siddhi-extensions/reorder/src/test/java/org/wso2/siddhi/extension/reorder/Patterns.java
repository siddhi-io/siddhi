package org.wso2.siddhi.extension.reorder;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Patterns {
    private File file;
    private BufferedWriter bw;

    public static void main(String[] args) throws InterruptedException {
        Patterns patterns = new Patterns();
        patterns.run();
    }

    public void run() throws InterruptedException {
        file = new File("/home/vithursa/Desktop/recent");

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FileWriter fw = null;
        try {
            fw = new FileWriter(file.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
        }

        bw = new BufferedWriter(fw);
        int count=0;
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream inputStream (sid int, eventtt long, x double, " +
                "y double, z int, v_abs double, a_abs int, vx int, vy int, vz int, ax int, " +
                "ay int, az int, iij_timestamp long); ";
        String interStream = "define stream interStream (sid int, eventtt long, x int, " +
                "y int, z int, v_abs int, a_abs int, vx int, vy int, vz int, ax int, " +
                "ay int, az int, iij_timestamp long); ";

        String outputStream = "define stream outputStream (sid int, eventtt long, x double, " +
                "y double, z int, v_abs double, a_abs int, vx int, vy int, vz int, ax int, " +
                "ay int, az int, iij_timestamp long); ";


        String query1 = ("@info(name = 'query1') from inputStream#reorder:akpattern(eventtt, v_abs, 10000l, \"from  e1=interStream[(x>29880 or x<22560) and y>-33968 and y<33965 and (sid==4 or sid ==12 or sid==10 or sid==8)] -> e2=interStream[(x<=29898 and x>22579) and y<=-33968 and z<2440 and a_abs>=55 and (sid==4 or sid ==12 or sid==10 or sid==8)]\") select sid, " +
                "eventtt, x, y, z, v_abs, a_abs, vx, vy, vz, ax, ay, az, iij_timestamp " +
                "insert into outputStream;");

        String query = ("@info(name = 'query1') from inputStream#reorder:aksequence(eventtt,v_abs,10000l, \"from e1=interStream[((sid==4 or sid==8 or sid==10 or sid==12) and (x>52483 or x<0 or y>33965 or y<-33960))],e2=interStream[((sid!=4 and sid!=8 and sid!=10 and sid!=12) and ((x>0 and x<1000) or (x<52483 and x>51483)) and ((y<33965 and y>32965) or (y>-33960 and y<-32960)))]\") select sid, " +
                "eventtt, x, y, z, v_abs, a_abs, vx, vy, vz, ax, ay, az, iij_timestamp " +
                "insert into outputStream;");


//        String query1 = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, v_abs, 100l) select sid, " +
//                "eventtt, x, y, z, v_abs, a_abs, vx, vy, vz, ax, ay, az, iij_timestamp " +
//                "insert into interStream;");
        String query2 = "@info(name = 'query2') from  e1=inputStream[(x>29880 or x<22560) and y>-33968 and y<33965" +
                " and (sid==4 or sid ==12 or sid==10 or sid==8)] -> " +
                "e2=inputStream[(x<=29898 and x>22579) and y<=-33968 and z<2440 and a_abs>=55000" +
                "and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
                "select e2.sid as sid, e2.eventtt as eventtt, e2.x as x,e2.y as y, e2.z as z," +
                " e2.v_abs as v_abs," +
                "e2.a_abs as a_abs, e2.vx as vx,e2.vy as vy, e2.vz as vz, e2.ax as ax," +
                " e2.ay as ay, e2.az as az, e1.iij_timestamp as iij_timestamp " +
                "insert into outputStream; ";

//        String query3 = "@info(name = 'query3') from e1=interStream[((sid==4 or sid==8 or sid==10 or sid==12) and (x>52483 or x<0 or y>33965 or y<-33960))],e2=interStream[((sid!=4 and sid!=8 and sid!=10 and sid!=12) and ((x>0 and x<1000) or (x<52483 and x>51483)) and ((y<33965 and y>32965) or (y>-33960 and y<-32960)))] " +
//                "select e2.sid as sid, e1.eventtt as eventtt, e2.x as x,e2.y as y, e2.z as z, e2.v_abs as v_abs," +
//                "e2.a_abs as a_abs, e2.vx as vx,e2.vy as vy, e2.vz as vz, e2.ax as ax, e2.ay as ay, e2.az as az, e1.iij_timestamp as iij_timestamp " +
//                "insert into outputStream; ";

        String query3 = "@info(name = 'query3') from e1=inputStream[((sid==4 or sid==8 or sid==10 or sid==12) and (x>52483 or x<0 or y>33965 or y<-33960))] -> e2=inputStream[((sid!=4 and sid!=8 and sid!=10 and sid!=12) and ((x>0 and x<1000) or (x<52483 and x>51483)) and ((y<33965 and y>32965) or (y>-33960 and y<-32960)))]" +
                "select e2.sid as sid, e2.eventtt as eventtt, e2.x as x,e2.y as y, e2.z as z, e2.v_abs as v_abs," +
                "e2.a_abs as a_abs, e2.vx as vx,e2.vy as vy, e2.vz as vz, e2.ax as ax, e2.ay as ay, e2.az as az, e1.iij_timestamp as iij_timestamp " +
                "insert into outputStream; ";

//        String query3 = "@info(name = 'query3') from  e1=interStream[(x>29880 or x<22560) and y>-33968 and y<33965 and (sid==4 or sid ==12 or sid==10 or sid==8)] -> e2=interStream[(x<=29898 and x>22579) and y<=-33968 and z<2440 and a_abs>=55 and (sid==4 or sid ==12 or sid==10 or sid==8)]" +
//                "select"

//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(
//                inputStream + outputStream + query1 + query2 );
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(
                inputStream + outputStream + query1);

//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(
//                inputStream + outputStream + query2);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        for (org.wso2.siddhi.core.event.Event event : events) {
//                           EventPrinter.print(events);
//                           event.getData()[13] = System.currentTimeMillis() -(Long) event.getData()[13];
//                            EventPrinter.print(events);
//                            System.out.println(difference);
                            try {
                                bw.write(""+event.getData()[0] + "," +
                                event.getData()[1] + "," +
                                event.getData()[2] + "," +
                                event.getData()[3] + "," +
                                event.getData()[4] + "," +
                                event.getData()[5] + "," +
                                event.getData()[6] + "," +
                                event.getData()[7] + "," +
                                event.getData()[8] + "," +
                                event.getData()[9] + "," +
                                event.getData()[10] + "," +
                                event.getData()[11] + "," +
                                event.getData()[12] +
                                        "");
                                bw.write("\r\n");
                                bw.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
        });

        DataLoader inputData = new DataLoader("/home/vithursa/Documents/DataFile/large_e2 delayed/out5th (copy)",500000);
        inputData.runSingleStream();
        LinkedBlockingQueue<Object> queueData = inputData.getEventBuffer();
        System.out.println(queueData.size());
        Iterator<Object> itrator = queueData.iterator();

        while(itrator.hasNext()){
            count+=1;
            try {
                Object[] obj = (Object[]) itrator.next();
                obj[13] = System.currentTimeMillis();
                inputHandler.send(obj);
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
