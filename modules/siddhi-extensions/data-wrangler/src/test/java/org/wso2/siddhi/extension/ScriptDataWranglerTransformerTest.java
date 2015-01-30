/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.siddhi.extension;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.io.FileHandler;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScriptDataWranglerTransformerTest {
	static final Logger log = Logger.getLogger(ScriptDataWranglerTransformer.class);

	private int count;
	private boolean eventArrived;
	private static SiddhiConfiguration siddhiConfiguration;

	@Before
	public void init() {
		count = 0;
		eventArrived = false;
	}
	@BeforeClass
	public static void setUp() throws Exception {
		log.info("Siddhi Environment will be created for testing !!");
		siddhiConfiguration = new SiddhiConfiguration();

		List<Class> extensions = new ArrayList<Class>(1);
		extensions.add(ScriptDataWranglerTransformer.class);
		log.info("ScriptDataWranglerTransformer was added successfully ");
		siddhiConfiguration.setSiddhiExtensions(extensions);
	ScriptDataWranglerTransformer.debuging=true;

	}

	@Test
	public void testTimeWindowQuery1() throws InterruptedException , IOException {

		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition()
		                                       .name("speed")
		                                       .attribute("vx", Attribute.Type.DOUBLE)
		                                       .attribute("vy", Attribute.Type.DOUBLE)
		                                       .attribute("vz", Attribute.Type.DOUBLE)
		                                       .attribute("v", Attribute.Type.DOUBLE));



		String script="function myfunction(x){ w = dw.wrangle()#n initial_transforms = dw.raw_inference(x).transforms;#n var data =dv.table(x); #n if(initial_transforms){ #n initial_transforms.forEach(function(t){#nw.add(t);#n})#nw.apply([data]);#n#n}#n#n/* Split data repeatedly on newline  into  rows */#nw.add(dw.split().column(['data'])#n.table(0)#n.status('active')#n.drop(true)#n.result('row')#n.update(false)#n.insert_position('right')#n.row(undefined)#n.on('#n')#n.before(undefined)#n.after(undefined)#n.ignore_between(undefined)#n.which(1)#n.max(0)#n.positions(undefined)#n.quote_character(undefined)#n)#n#n/* Split data repeatedly on ',' */#nw.add(dw.split().column(['data'])#n.table(0)#n.status('active')#n.drop(true)#n.result('column')#n.update(false)#n.insert_position('right')#n.row(undefined)#n.on(',')#n.before(undefined)#n.after(undefined)#n.ignore_between(undefined)#n.which(1)#n.max(0)#n.positions(undefined)#n.quote_character(undefined)#n)#n#n/* Drop split */#nw.add(dw.drop().column(['split'])#n.table(0)#n.status('active')#n.drop(true)#n)#n#n/* apply transforms to data.  data can be a string, datatable or array of datatables */#nw.apply([data])#n#nreturn dw.wrangler_export(data,{});#n}";
		String outputStreamDefinition="define stream velocityStream (velocityX double , velocityY double , velocityZ double)";

		String query = "from speed#transform.dataWrangler:wrangleScript(\""+"\",\""+outputStreamDefinition+"\") "+
		                 "select * "+
		                 "insert into velocityStream";

		//log.info(query);
		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
			                    Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {
					count++;
					eventArrived=true;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("speed");
		inputHandler.send(new Object[] { 10,55.6,11,3 });
		inputHandler.send(new Object[] { 20,65.6,23.3 ,3});
		Thread.sleep(500);
		inputHandler.send(new Object[] { 302,75.6,23,12 });
		Thread.sleep(6000);
		// Assert.assertEquals("In and Remove events has to be equal", 0,
		// count);
		log.info("count "+count);
		Assert.assertEquals("Event arrived", true, eventArrived);
		siddhiManager.shutdown();
	}
}