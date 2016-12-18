package org.wso2.siddhi.extension.reorder;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;



//import static com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text.NEW_LINE;

/**
 * This is the test case for KSlackExtension.
 * Created by miyurud on 8/10/15.
 */
public class TestCase {
    static final Logger log = Logger.getLogger(AlphaKSlackExtensionTestCase.class);
    private File file;
    private BufferedWriter bw;
    private String content;
    private int counter =0;
    private int count =0;
    private long sum=0;


    @Before
    public void init() {
    }

    @Test
    public void OrderTest() throws InterruptedException, IOException {
        file = new File("/home/vithursa/Desktop/current/eak5");

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

        log.info("AlphaKSlackExtensionTestCase TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (sid int, eventtt long, x int, y int, z int, " +
                "v_abs double, a_abs int, vx int, vy int, vz int, ax int, ay int, az int,iij_timestamp long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, v_abs) select sid, " +
                "eventtt, x, y, z, v_abs, a_abs, vx, vy, vz, ax, ay, az, iij_timestamp " +
                "insert into outputStream;");
//        String inStreamDefinition = "define stream inputStream (ts long, data double, data2 double);";
//        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(ts, data) select ts, data, data2 " +
//                "insert into outputStream;");

        /*String query = ("@info(name = 'query1') from every e1 = inputStream[eventtt>0]-> e2 = inputStream[eventtt>e1.eventtt]" +
                "select e2.sid as sid, e2.eventtt as eventtt, e2.x as x, e2.y as y, e2.z as z, e2.v_abs as v_abs, e2.a_abs as a_abs, " +
                "e2.vx as vx, e2.vy as vy, e2.vz as vz, e2.ax as ax, e2.ay as ay, e2.az as az " +
                "insert into outputStream;");*/

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    long difference = System.currentTimeMillis() - (Long)event.getData()[13];
                    sum += difference;
                    count+=1;
                    //System.out.println(count);
                    if(count>=10000){
//                        System.out.println(sum*1.0/count);
                        sum =0;
                        count=0;
                        //counter=0;
                    }

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
                                event.getData()[12] + //"," +
//                                (difference)+
                                "");
                        bw.write("\r\n");
                        bw.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        DataLoader inputData = new DataLoader("/home/vithursa/Desktop/OOEventsCopy", 500000);
        inputData.runSingleStream();
        LinkedBlockingQueue<Object> events = inputData.getEventBuffer();
        Iterator<Object> itr = events.iterator();
        while(itr.hasNext()){
            //System.out.println(counter);
            Object[] obj = (Object[]) itr.next();
            obj[13] = System.currentTimeMillis();
            inputHandler.send(obj);
//            counter+=1;
//            if(counter==50000){
//                Thread.sleep(10000);
//                counter=0;
//            }
        }
        Thread.sleep(2000);
        executionPlanRuntime.shutdown();

    }
}