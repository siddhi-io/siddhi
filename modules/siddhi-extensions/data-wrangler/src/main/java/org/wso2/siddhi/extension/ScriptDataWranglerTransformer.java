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
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.extension.exceptions.InvalidInputFormatException;
import org.wso2.siddhi.extension.io.FileHandler;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.List;

/**
 *
 * This class can wrangle the events based on the JS script passed as the 1st parameter in siddhi query.
 * Second parameter should be the definition of the output stream of transform processor.
 * <p/>
 * Usage:
 * from streamA#transform:datawrangler.wrangleScript('js Code goes here','define statement')
 * Select .....
 * <p/>
 * Created by tharindu on 1/23/15.
 *
 */

@SiddhiExtension(namespace = "dataWrangler", function = "wrangleScript")
public class ScriptDataWranglerTransformer extends DataWranglerTransformProcessor {

	private static Logger logger = Logger.getLogger(ScriptDataWranglerTransformer.class);

	public static boolean debuging=false;
	@Override
	protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {
		StreamDefinition definition;
		String script="";
		if (expressions.length == 2)
			try {

				if (!debuging) {
					script = ((StringConstant) expressions[0])
							.getValue();  //first parameter is the script
					logger.debug("before " + script);
					script = script.replace('#', '\\');
					logger.debug("After  " + script);
				} else {

					script = FileHandler.readScript("testScript.js");
					logger.debug(script);
				}

				Expression exp =
						expressions[1];    //second parameter should be the output definition

				definition =
						SiddhiCompiler.parseStreamDefinition(((StringConstant) exp).getValue());

				initialization(script, definition);  //initialize the transform processor

			} catch (SiddhiParserException | ScriptException e) {
				logger.error(e);
				throw new RuntimeException(e);
			} catch (IOException e) {
				logger.error(e);
				throw new RuntimeException(e);
			}
		else {


			logger.error("Number of inputs should be exactly 2 \n" +
			             "first: JS_CODE \nsecond: DEFINE_STATEMENT_OF_OUTPUT_STREAM");
			throw new InvalidInputFormatException(
					"Number of inputs should be exactly 2 \n" +
					"first: JS_CODE \nsecond: DEFINE_STATEMENT_OF_OUTPUT_STREAM");
		}

	}
}
