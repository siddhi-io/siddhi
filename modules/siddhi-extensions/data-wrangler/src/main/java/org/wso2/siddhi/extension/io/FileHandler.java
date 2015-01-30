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
import org.wso2.siddhi.extension.DataWranglerTransformProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * FileHandler Class will read resource files and return the contents as  texts
 * Created by tharindu on 1/21/15.
 */

public class FileHandler {

	//Attributes .............................................................
	private static Logger logger = Logger.getLogger(FileHandler.class);

	/**
	 * This method read the content of a file inside the resouces folder( dw.js, lib/*)
	 *
	 * @param fileName : Name of the file you want to read (within the resources folder)
	 * @return : content of the file as a String
	 */

	public static String readScript(String fileName) throws IOException {
		InputStream in = null;
		BufferedReader reader = null;
		StringBuilder out = new StringBuilder();
		try {
			in = DataWranglerTransformProcessor.class.getResourceAsStream(fileName);
			reader = new BufferedReader(new InputStreamReader(in));

			String line;

			while ((line = reader.readLine()) != null) {
				out.append(line).append("\n");
			}
			logger.debug(fileName + " is added Successfully");
			return out.toString();

		} catch (IOException e) {

			logger.error("Error occurred in FileHandler.readScript(" + fileName + ")\n" + e);
			throw e;
		} finally {
			if (reader != null)
				reader.close();
			if (in != null)
				in.close();
		}

	}

}
