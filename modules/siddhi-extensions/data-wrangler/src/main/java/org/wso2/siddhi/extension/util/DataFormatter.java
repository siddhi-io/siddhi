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
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a Utility class that can marshaling and unmarshalling , OBJECTS <-> CSV
 * Created by tharindu on 1/20/15.
 */

public class DataFormatter {

	//Attributes ..................................................................

	private static Logger logger = Logger.getLogger(DataFormatter.class);

	//Methods goes here...........................................................

	/**
	 * This method convert list of java objects into CSV
	 *
	 * @param array : List of objects that need to be converted to CSV
	 * @return : csv that represent the list of objects
	 */
	public static String objectArrayToCSV(Object array[]) {
		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < array.length; i++) {
			builder.append(array[i].toString());
			if (i != array.length - 1)
				builder.append(",");
		}
		logger.info("Event ");
		return builder.toString();
	}

	/**
	 * This method convert a list of csv into Plain java Objects according to the output Stream definition
	 *
	 * @param csv : comma
	 * @param def : outputStream definition
	 * @return : list of POJOs
	 */
	public static Object[] csvToObjectArray(String csv, StreamDefinition def) {

		String[] output = csv.split(",");

		List<Attribute> attributes = def.getAttributeList();
		List<Object> list = new ArrayList<>();
		for (int i = 0; i < output.length; i++) {
			list.add(i, converter(output[i], attributes.get(i).getType()));
		}
		return list.toArray();
	}

	/**
	 * This method cast a raw string into given data type object
	 *
	 * @param str : string need to be cast
	 * @param def : Type of the output
	 * @return : converted object
	 */
	private static Object converter(String str, Type def) {
		Object value;
		switch (def) {
			case INT:
				value = Integer.valueOf(str);
				break;
			case DOUBLE:
				value = Double.valueOf(str);
				break;
			case FLOAT:
				value = Float.valueOf(str);
				break;
			case BOOL:
				value = Double.valueOf(str);
				break;
			case LONG:
				value = Long.valueOf(str);
				break;
			default:
				value = str;
		}
		return value;
	}

}
