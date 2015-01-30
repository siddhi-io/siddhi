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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.extension.io.RegistryClient;
import org.wso2.siddhi.extension.util.DataFormatter;
import org.wso2.siddhi.extension.util.ScriptExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class doesn't provide concrete implementation of init method and annotations
 * But provides the common behavior of 2 concrete classes
 */

public abstract class DataWranglerTransformProcessor extends TransformProcessor {

	//Attributes ..............................................................................

	private Map<String, Integer> paramPositions ;
	private static Logger logger = Logger.getLogger(DataWranglerTransformProcessor.class);
	private ScriptExecutor engine;                                    //JS execution environment
	private String script;                                             //Script as a String
	private RegistryClient registry;           //for registry interaction

	private String context;

	//Getters and Setters ......................................................................

	public ScriptExecutor getEngine() {
		return engine;
	}

	public void setEngine(ScriptExecutor engine) {
		this.engine = engine;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public RegistryClient getRegistry() {
		return registry;
	}

	public String getContext() {
		return context;
	}

	public void setContext(String context) {
		this.context = context;
	}



	//Methods.....................................................................................


	public DataWranglerTransformProcessor(){
		registry = new RegistryClient();
		paramPositions=new HashMap<String, Integer>();
	}

	/**
	 * This method is executed for each event (this is the method that performs any transformation)
	 *
	 * @param inEvent :input Event Object
	 * @return :transformed event object
	 */
	@Override
	protected InStream processEvent(InEvent inEvent) {
		Object[] data ;
		try {
			String csv = DataFormatter.objectArrayToCSV(inEvent.getData());
			logger.debug(getContext()+" InProcessEvent()-> input CSV: "+csv);

			//Convert the event payload to CSV and send as a parameter to the JS CODE
			String outputString =
					((String) engine.execute(csv));

			//remove the unwanted strings withing the output
			outputString = outputString
					.substring(outputString.indexOf("\n") + 1, outputString.lastIndexOf("\n"));
			logger.debug(getContext()+" InProcessEvent()-> output CSV: "+outputString);

			//convert the output into objects according to the outputStream definitions
			data = DataFormatter.csvToObjectArray(outputString, this.getOutStreamDefinition());

			List<Attribute> definitions = outStreamDefinition.getAttributeList();
			for (int i = 0; i < data.length; i++) {

				logger.info(definitions.get(i).getName() + " (" + definitions.get(i).getType() +
				            ")  : " + data[i].toString());
			}

			return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), data);
		} catch (Exception e) {
			logger.error("Error occurred inProcess Event Method" + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();

		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {

				transformedListEvent.addEvent((Event) processEvent((InEvent) event));
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		return new Object[] { paramPositions };
	}

	@Override
	protected void restoreState(Object[] objects) {
		if (objects.length > 0 && objects[0] instanceof Map) {
			paramPositions = (Map<String, Integer>) objects[0];
		}
	}

	/**
	 * This method setup the script ,JS environment and  output stream definition
	 *
	 * @param script              : Script need to be executed
	 * @param outStreamDefinition : OutputStream definition of transformer
	 */
	public void initialization(String script, StreamDefinition outStreamDefinition)
			throws ScriptException, IOException {
		try {
			this.script = script;
			logger.info("Script in Initialization method" + script);

			this.outStreamDefinition = outStreamDefinition;

			engine = new ScriptExecutor(script);
			logger.info("Script loaded successfully");
		} catch (ScriptException ex) {
			logger.error("Script Exception occurred in engine" + ex.getMessage());
			throw ex;

		} catch (IOException ex) {
			logger.error("IOException occurred in engine " + ex.getMessage());
			throw ex;

		}
	}

	@Override
	public void destroy() {

	}
}
