/*Copyright 2018 modakanalytics.com

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.*/
package com.modak.dataspider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.modak.dataspider.utils.JSONUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import com.modak.dataspider.dbutils.LoggerUtils;

/**
 * This class is a factory pattern implementation for MetadataFetcher entities
 * and is responsible to create and return one of MetadataFetcher class implementations based on the supplied Source
 * @author modakanalytics
 * @version 1.0
 */
public class MetadataFetcherFactory {
	protected HashMap<String, String> classMapping = null;
	HashMap<String, String> koshAttributes = null;
	HashMap<String, String> runAttributes = new HashMap<String, String>();
	static String resources_path = null;
	private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger("MetadataFetcherFactory");

	public void init(HashMap<String, String> koshAttributes, HashMap<String, String> runAttributes,
		String resources_path) {
		try {
			this.koshAttributes = koshAttributes;
			this.runAttributes = runAttributes;
			MetadataFetcherFactory.resources_path = resources_path;
			// load the JSON file
			File metadataFetcherMappingFile = new File(resources_path + File.separator + "run_configurations"
				+ File.separator + "MetadataFetcherMapping.json");
			String metadataFetcherMapping_JSONString = FileUtils.readFileToString(metadataFetcherMappingFile,
				Charset.defaultCharset());
			classMapping = new HashMap<String, String>();
			Map<String, Object> mapping = JSONUtils.jsonToMap(metadataFetcherMapping_JSONString);
			mapping.forEach((k, v) -> classMapping.put(k, coalesce(v).toString()));
			//if exception caught, inserting into log table
		} catch (FileNotFoundException fne) {
			fne.printStackTrace();
			LOGGER.error("File MetadataFetcherMapping.json is not found\n" + fne);
			LoggerUtils.insertLog(this.getClass().getSimpleName(),
				"File MetadataFetcherMapping.json is not found\n" + fne, true);
		} catch (IOException io) {
			io.printStackTrace();
			LOGGER.error("IOException while reading MetadataFetcherMapping.json\n" + io);
			LoggerUtils.insertLog(this.getClass().getSimpleName(),
				"IOException while reading MetadataFetcherMapping.json\n" + io, true);
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Error while executing init() method of MetadataFetcherFactory\n" + ex);
			LoggerUtils.insertLog(this.getClass().getSimpleName(),
				"Error while executing init() method of MetadataFetcherFactory\n" + ex, true);
		}
	}

	/**
	 *  this method intialize the metadatafetcher and return a instance of it
	 * @param connectAttributes
	 * @param crawljobno current crawl_job _no for the run
	 * @return instance of the metadata fetcher
	 */

	public MetadataFetcher getInstance(ConnectAttributes connectAttributes, int crawljobno) {

		if (connectAttributes.containsKey("connection_template")) {
			String className = classMapping.get(connectAttributes.get("connection_template"));
			if (className != null) {
				try {
					MetadataFetcher fetcher = (MetadataFetcher) Class.forName(className).newInstance();
					fetcher.init(connectAttributes, koshAttributes, runAttributes, crawljobno);
					return fetcher;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				// throw error and log that in Logger
			}
		} else {
			// throw error and log that in Logger
		}
		return null;
	}

	/**
	 *
	 * @param obj
	 * @return the object as string or ""
	 */
	public Object coalesce(Object obj) {
		if (obj == null) {
			return "";
		} else {
			return obj.toString();
		}
	}
}
