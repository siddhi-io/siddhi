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
import org.wso2.carbon.registry.api.RegistryException;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.extension.exceptions.InvalidInputFormatException;
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
 * This class can wrangle the events based on the WRANGLER CONFIGURATIONS saved in wso2 registry
 * Once the user graphically wrangle event using carbon component inside the Admin console , Generated JS CODE and OUTPUT DEFINITION will be stored in folder with a GIVEN NAME
 * This transform process will transform the events based on that CONFIGURATION
 * <p/>
 * Usage:
 * CONFIGURATION NAME should be passed as a parameter
 * 1)    from streamA#transform:datawrangler.wrangleConfig('CONFIGURATION NAME')
 * Select .....
 * 2)    from streamA#transform:datawrangler.wrangleConfig('CONFIGURATION NAME','Define statement for output stream')
 * Select .....
 * <p/>
 * Created by tharindu on 1/23/15.
 */

@SiddhiExtension(namespace = "dataWrangler", function = "wrangleConfig")
public class ConfigDataWranglerTransformer extends DataWranglerTransformProcessor {

	private static Logger logger = Logger.getLogger(ConfigDataWranglerTransformer.class);

	@Override
	protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {

		StreamDefinition definition;
		String script;

		// if only 2 parameters presents
		if (expressions.length <= 2 &&
		    expressions.length > 0)
			try {
				String configName = ((StringConstant) expressions[0])
						.getValue();   //first parameter should be the configuration name
				script = getRegistry()
						.getScript(configName);                       //get the script in registry
				String outputStreamDef;
				if (expressions.length == 1) {
					// if only one parameter presents=> use the output definition in registry
					outputStreamDef = getRegistry().getOutputStreamDefinition(configName);
					setContext("ConfigDataWranglerTransformer: (1 parameter)");
				} else {
					// if exactly 2 parameters presents=> second parameter should be the output definition
					outputStreamDef = ((StringConstant) expressions[1]).getValue();
					setContext("ConfigDataWranglerTransformer: (2 parameter)");

				}

				definition = SiddhiCompiler.parseStreamDefinition(outputStreamDef);

				initialization(script, definition);

			} catch (RegistryException | SiddhiParserException | IOException | ScriptException ex) {
				logger.error(ex);
				throw new RuntimeException(ex);
			}
		else {
			throw new InvalidInputFormatException(
					"Number of inputs should be less than or equal to 2 \n" +
					"first: CONFIGURATION_NAME\nsecond: DEFINE_STATEMENT_OF_OUTPUT_STREAM(Optional)");
		}
	}
}
