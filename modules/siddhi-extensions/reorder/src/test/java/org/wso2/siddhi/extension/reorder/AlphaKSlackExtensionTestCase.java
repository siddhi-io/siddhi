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
 * Created by vithursa on 10/5/16.
 */
public class AlphaKSlackExtensionTestCase {
    static final Logger log = Logger.getLogger(AlphaKSlackExtensionTestCase.class);
    private File file;
    private BufferedWriter bw;
    private String content;
    private long sum=0;
    private int counter =0;
    private int count =0;


    @Before
    public void init() {
    }

    @Test
    public void OrderTest() throws InterruptedException, IOException {
        file = new File("/home/vithursa/Desktop/Today/AKSlackDelay");

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
                "v_abs int, a_abs int, vx int, vy int, vz int, ax int, ay int, az int, iij_timestamp long);";
        String query = ("@info(name = 'query1') from inputStream #reorder:akslack(eventtt,v_abs) select sid, " +
                "eventtt, x, y, z, v_abs, a_abs, vx, vy, vz, ax, ay, az, iij_timestamp " +
                "insert into outputStream;");

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
                        //System.out.println(sum*1.0/count);
                        //System.out.println(sum*1.0/count);
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


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        DataLoader inputData = new DataLoader("/home/vithursa/Desktop/OOEventsDelay",500000);
        inputData.runSingleStream();
        LinkedBlockingQueue<Object> events = inputData.getEventBuffer();
        Iterator<Object> itr = events.iterator();
        while(itr.hasNext()){
            Object[] obj = (Object[]) itr.next();
            obj[13] = System.currentTimeMillis();
            inputHandler.send(obj);
        }
        Thread.sleep(2000);
        executionPlanRuntime.shutdown();

    }
}
