/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.managment;

import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.Operation;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.util.snapshot.state.Snapshot;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class SnapshotableEventQueueTestCase {
    private static final Logger log = LogManager.getLogger(SnapshotableEventQueueTestCase.class);

    /**
     * Read the object from Base64 string.
     */
    private static Object fromString(String s) throws IOException,
            ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /**
     * Write the object to a Base64 string.
     */
    private static String toString(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    @BeforeMethod
    public void init() {
    }

    @Test
    public void incrementalPersistenceTest1() throws InterruptedException, IOException, ClassNotFoundException {

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(new Attribute("symbol", Attribute.Type.STRING));
        metaStreamEvent.addOutputData(new Attribute("price", Attribute.Type.FLOAT));
        metaStreamEvent.addOutputData(new Attribute("volume", Attribute.Type.LONG));

        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent,
                new StreamEventFactory(metaStreamEvent));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setOutputData(new Object[]{"IBM", 500.6f, 1});

        for (int i = 0; i < 20; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        HashMap<Long, String> snapshots = new HashMap<>();
        Snapshot snapshot1 = snapshotableStreamEventQueue.getSnapshot();
        StreamEvent streamEvents = (StreamEvent) snapshot1.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots.put(3L, toString(snapshot1));

        for (int i = 20; i < 40; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot2 = snapshotableStreamEventQueue.getSnapshot();
        ArrayList<Operation> operationLog = (ArrayList<Operation>) snapshot2.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(4L, toString(snapshot2));

        for (int i = 40; i < 80; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot3 = snapshotableStreamEventQueue.getSnapshot();
        operationLog = (ArrayList<Operation>) snapshot3.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(5L, toString(snapshot3));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue2 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        SnapshotStateList snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue2.restore(snapshotStateList);

        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue2);

        for (int i = 80; i < 130; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot4 = snapshotableStreamEventQueue.getSnapshot();
        streamEvents = (StreamEvent) snapshot4.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots = new HashMap<>();
        snapshots.put(6L, toString(snapshot4));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue3 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue3.restore(snapshotStateList);
        snapshotableStreamEventQueue.reset();
        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue3);
    }

    @Test
    public void incrementalPersistenceTest2() throws InterruptedException, IOException, ClassNotFoundException {

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(new Attribute("symbol", Attribute.Type.STRING));
        metaStreamEvent.addOutputData(new Attribute("price", Attribute.Type.FLOAT));
        metaStreamEvent.addOutputData(new Attribute("volume", Attribute.Type.LONG));

        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent,
                new StreamEventFactory(metaStreamEvent));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setOutputData(new Object[]{"IBM", 500.6f, 1});

        for (int i = 0; i < 10; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        HashMap<Long, String> snapshots = new HashMap<>();
        Snapshot snapshot1 = snapshotableStreamEventQueue.getSnapshot();
        StreamEvent streamEvents = (StreamEvent) snapshot1.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots.put(3L, toString(snapshot1));

        for (int i = 7; i < 10; i++) {
            snapshotableStreamEventQueue.next();
            snapshotableStreamEventQueue.remove();
        }

        Snapshot snapshot2 = snapshotableStreamEventQueue.getSnapshot();
        ArrayList<Operation> operationLog = (ArrayList<Operation>) snapshot2.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(4L, toString(snapshot2));

        for (int i = 10; i < 15; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot3 = snapshotableStreamEventQueue.getSnapshot();
        operationLog = (ArrayList<Operation>) snapshot3.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(5L, toString(snapshot3));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue2 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        SnapshotStateList snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue2.restore(snapshotStateList);

        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue2);
    }

    @Test
    public void incrementalPersistenceTest3() throws InterruptedException, IOException, ClassNotFoundException {

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(new Attribute("symbol", Attribute.Type.STRING));
        metaStreamEvent.addOutputData(new Attribute("price", Attribute.Type.FLOAT));
        metaStreamEvent.addOutputData(new Attribute("volume", Attribute.Type.LONG));

        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent,
                new StreamEventFactory(metaStreamEvent));
        SnapshotableStreamEventQueue snapshotableStreamEventQueue =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setOutputData(new Object[]{"IBM", 500.6f, 1});

        for (int i = 0; i < 10; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        HashMap<Long, String> snapshots = new HashMap<>();
        Snapshot snapshot1 = snapshotableStreamEventQueue.getSnapshot();
        StreamEvent streamEvents = (StreamEvent) snapshot1.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots.put(3L, toString(snapshot1));

        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();

        for (int i = 7; i < 10; i++) {
            snapshotableStreamEventQueue.next();
            snapshotableStreamEventQueue.remove();
        }

        Snapshot snapshot2 = snapshotableStreamEventQueue.getSnapshot();
        ArrayList<Operation> operationLog = (ArrayList<Operation>) snapshot2.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(4L, toString(snapshot2));

        for (int i = 10; i < 15; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot3 = snapshotableStreamEventQueue.getSnapshot();
        operationLog = (ArrayList<Operation>) snapshot3.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(5L, toString(snapshot3));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue2 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        SnapshotStateList snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue2.restore(snapshotStateList);
        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue2);
    }

    @Test
    public void incrementalPersistenceTest4() throws InterruptedException, IOException, ClassNotFoundException {

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(new Attribute("symbol", Attribute.Type.STRING));
        metaStreamEvent.addOutputData(new Attribute("price", Attribute.Type.FLOAT));
        metaStreamEvent.addOutputData(new Attribute("volume", Attribute.Type.LONG));

        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent,
                new StreamEventFactory(metaStreamEvent));
        SnapshotableStreamEventQueue snapshotableStreamEventQueue =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setOutputData(new Object[]{"IBM", 500.6f, 1});

        for (int i = 0; i < 10; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        HashMap<Long, String> snapshots = new HashMap<>();
        Snapshot snapshot1 = snapshotableStreamEventQueue.getSnapshot();
        StreamEvent streamEvents = (StreamEvent) snapshot1.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots.put(3L, toString(snapshot1));

        snapshotableStreamEventQueue.poll();
        snapshotableStreamEventQueue.poll();

        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();

        for (int i = 7; i < 10; i++) {
            snapshotableStreamEventQueue.poll();
        }

        Snapshot snapshot2 = snapshotableStreamEventQueue.getSnapshot();
        ArrayList<Operation> operationLog = (ArrayList<Operation>) snapshot2.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(4L, toString(snapshot2));

        for (int i = 10; i < 15; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot3 = snapshotableStreamEventQueue.getSnapshot();
        operationLog = (ArrayList<Operation>) snapshot3.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(5L, toString(snapshot3));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue2 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        SnapshotStateList snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue2.restore(snapshotStateList);
        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue2);
    }

    @Test
    public void incrementalPersistenceTest5() throws InterruptedException, IOException, ClassNotFoundException {

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(new Attribute("symbol", Attribute.Type.STRING));
        metaStreamEvent.addOutputData(new Attribute("price", Attribute.Type.FLOAT));
        metaStreamEvent.addOutputData(new Attribute("volume", Attribute.Type.LONG));

        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent,
                new StreamEventFactory(metaStreamEvent));
        SnapshotableStreamEventQueue snapshotableStreamEventQueue =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setOutputData(new Object[]{"IBM", 500.6f, 1});

        for (int i = 0; i < 10; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        HashMap<Long, String> snapshots = new HashMap<>();
        Snapshot snapshot1 = snapshotableStreamEventQueue.getSnapshot();
        StreamEvent streamEvents = (StreamEvent) snapshot1.getState();
        Assert.assertTrue(streamEvents != null);
        snapshots.put(3L, toString(snapshot1));

        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();
        snapshotableStreamEventQueue.next();

        for (int i = 7; i < 10; i++) {
            snapshotableStreamEventQueue.next();
            streamEvent.getOutputData()[2] = i + 20;
            snapshotableStreamEventQueue.overwrite(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot2 = snapshotableStreamEventQueue.getSnapshot();
        ArrayList<Operation> operationLog = (ArrayList<Operation>) snapshot2.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(4L, toString(snapshot2));

        for (int i = 10; i < 15; i++) {
            streamEvent.getOutputData()[2] = i;
            snapshotableStreamEventQueue.add(streamEventCloner.copyStreamEvent(streamEvent));
        }

        Snapshot snapshot3 = snapshotableStreamEventQueue.getSnapshot();
        operationLog = (ArrayList<Operation>) snapshot3.getState();
        Assert.assertTrue(operationLog != null);
        snapshots.put(5L, toString(snapshot3));

        SnapshotableStreamEventQueue snapshotableStreamEventQueue2 =
                new SnapshotableStreamEventQueue(new StreamEventClonerHolder(streamEventCloner));
        SnapshotStateList snapshotStateList = new SnapshotStateList();
        for (Map.Entry<Long, String> entry : snapshots.entrySet()) {
            snapshotStateList.putSnapshotState(entry.getKey(), (Snapshot) fromString(entry.getValue()));
        }
        snapshotableStreamEventQueue2.restore(snapshotStateList);
        Assert.assertEquals(snapshotableStreamEventQueue, snapshotableStreamEventQueue2);
    }

}
