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

package org.wso2.siddhi.extension.util;

import org.apache.log4j.Logger;
import org.wso2.siddhi.extension.io.FileHandler;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * This class Execute the generated JS code within a JS Environment
 * <p/>
 * Created by tharindu on 1/19/15.
 */

public class ScriptExecutor {

	private static Logger logger = Logger.getLogger(ScriptExecutor.class);
	private ScriptEngine engine;

	/**
	 * This method load dynamically the JS code need to be executed (When init() method executed)"
	 *
	 * @param script : Script need to be executed
	 * @throws IOException
	 * @throws ScriptException
	 */
	public ScriptExecutor(String script) throws IOException, ScriptException {

		try {

			ScriptEngineManager factory = new ScriptEngineManager();

			engine =
					factory.getEngineByName(
							Parameters.SCRIPT_TYPE);            //    create JavaScript engine

			//Load the dependent libraries to the engine
			engine.eval(FileHandler.readScript(Parameters.JS_DEPENDENCY_LIB1));
			engine.eval(FileHandler.readScript(Parameters.JS_DW_LIB));
			engine.eval(script);
			logger.debug("All the Script files are added successfully");
		} catch (ScriptException ex) {

			logger.error("ScriptException occurred in ScriptExecutor(" + script + ")" + ex);
			throw ex;
		} catch (IOException ex) {
			logger.error("IOException occurred in ScriptExecutor(" + script + ")" + ex);
			throw ex;

		}

	}

	/**
	 * This method execute the JS code as a function (For each event -> Inprocess() )
	 *
	 * @param parameter : Parameter for the JS code
	 * @return : the resultant object
	 * @throws NoSuchMethodException
	 * @throws ScriptException
	 */

	public Object execute(String parameter) throws NoSuchMethodException, ScriptException {
		try {
			return ((Invocable) engine).invokeFunction(Parameters.JS_FUNCTION_NAME, parameter);
		} catch (NoSuchMethodException ex) {
			logger.error("NoSuchMethodException occurred in ScriptExecutor.execute(" + parameter +
			             ")\n" + ex);
			throw ex;
		} catch (ScriptException ex) {
			logger.error(
					"ScriptException occurred in ScriptExecutor.execute(" + parameter + ")\n" + ex);
			throw ex;
		}
	}

}
