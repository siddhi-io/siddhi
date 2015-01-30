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

package org.wso2.siddhi.extension.io;

import org.apache.log4j.Logger;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.context.RegistryType;
import org.wso2.carbon.registry.api.Registry;
import org.wso2.carbon.registry.api.RegistryException;
import org.wso2.carbon.registry.api.Resource;
import org.wso2.siddhi.extension.util.Parameters;

/**
 * This class handle the interactions with WSO2 CEP registry
 * Created by tharindu on 1/20/15.
 */

public class RegistryClient {

	//Attributes ..................................................................

	private static Logger logger = Logger.getLogger(RegistryClient.class);
	private Registry registry;

	public RegistryClient() {
		CarbonContext cCtx = CarbonContext.getCurrentContext();
		String registryType = RegistryType.SYSTEM_GOVERNANCE.toString();
		registry = cCtx.getRegistry(RegistryType.valueOf(registryType));
	}

	//Method ......................................................................

	/**
	 * This method return the JS script for given configuration
	 *
	 * @param name : name of the Script Configuration
	 * @return : content of the JS code as a string
	 */
	public String getScript(String name) throws RegistryException {
		try {
			String output = getText(name, Parameters.SCRIPT_NAME);

			logger.info("Script " + name + "  is read successfully ");
			return output;

		} catch (RegistryException ex) {
			logger.error("Exception occurred in RegistryClient.getOutputStreamDefinition(" + name +
			             ")\n" + ex);
			throw ex;
		}

	}

	/**
	 * This method read the output stream definition of a given configuration name
	 *
	 * @param name: name of the Configuration in registry
	 * @return : content of the definition file as a String
	 */
	public String getOutputStreamDefinition(String name) throws RegistryException {
		try {
			String output = getText(name, Parameters.CONFIG_NAME);

			logger.info("outputStream Definition " + name + " is read successfully ");
			return output;

		} catch (RegistryException ex) {
			logger.error("Exception occurred in RegistryClient.getOutputStreamDefinition(" + name +
			             ")\n" + ex);
			throw ex;
		}
	}

	/**
	 * This is a generic method to read a file from the registry
	 *
	 * @param folderName : name of the folder (which is same as configuration name)
	 * @param fileName   : config.json | script.js
	 * @return : content of the file as a String
	 * @throws RegistryException
	 */
	private String getText(String folderName, String fileName) throws RegistryException {

		try {
			String contentPath = Parameters.BASE_URL + folderName + "/" + fileName;
			logger.debug("Retrieving path :" + contentPath);
			Resource resource = registry.get(contentPath);

			byte[] streamOfBytes = (byte[]) resource.getContent();
			String content = new String(streamOfBytes);
			logger.debug("Content of the " + contentPath + " is :\n" + content);

			return content;

		} catch (RegistryException e) {
			logger.error(
					"Exception occurred in RegistryClient.getText(" + folderName + "," + fileName +
					") \n" + e);
			throw e;
		}
	}

}
