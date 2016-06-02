/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.nio.charset.StandardCharsets;
import com.cruzdb.Log;


/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class ZlogClient extends DB
{
	public static final String VERBOSE="zlogdb.verbose";
	public static final String VERBOSE_DEFAULT="true";
        public Log log;	
    	boolean verbose;
	public static Map<String, String> splitToMap(String source,
                                                     String entriesSeparator,
                                                     String keyValueSeparator) {
    		Map<String, String> map = new HashMap<String, String>();
    		String[] entries = source.split(entriesSeparator);
    		for (String entry : entries) {
        		if (entry.isEmpty() == false && entry.contains(keyValueSeparator)) {
            			String[] keyValue = entry.split(keyValueSeparator);
            			map.put("field" + keyValue[0], keyValue[1]);
        		}
    		}		
   		 return map;
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@SuppressWarnings("unchecked")
	public void init()
	{
                /* Connect to sequencer */
                /* Create log object */
		String logname = "abc";
		try {
			log = Log.open("rbd", "localhost", 5678, logname);
       		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public Status read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
	{       
		byte[] values = "".getBytes();
		try {
			values = log.kvread(key);
		} catch (Exception exc) {
                        exc.printStackTrace();
                }
                String value_str = new String(values, StandardCharsets.UTF_8);
                //System.out.println(value_str);
		Map<String, String> extractedMap = splitToMap(value_str, " field", "=value");
                //System.out.println(extractedMap);
		if (fields == null) {
			StringByteIterator.putAllAsByteIterators(result, extractedMap);
		}
		return result.isEmpty() ? Status.ERROR : Status.OK;
	}
	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public Status insert(String table, String key, HashMap<String,ByteIterator> values)
	{
		// field_value is a map of type <String, String>
		//Map<String, String> field_value = StringByteIterator.getStringMap(values);
		String data = new String("");

		// concat key, val pairs, separators added  
		for (Map.Entry<String, ByteIterator> entry : values.entrySet())
		{
			data += " " + entry.getKey() + "=value" + entry.getValue();
		}
		byte b[] = data.getBytes(StandardCharsets.UTF_8);
		try {
			long ret = log.kvinsert(key, b);
			if (ret != 0)
				return Status.ERROR;
		} catch (Exception exc) {
                        exc.printStackTrace();
                }
		return Status.OK;	
	}

	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
	{
                return Status.OK;
        }

	public Status update(String table, String key, HashMap<String,ByteIterator> values)
	{
		return Status.OK;
	}

	public Status delete(String table, String key)
	{
		return Status.OK;
	}
	/**
	 * Short test of BasicDB
	 */
	/*
	public static void main(String[] args)
	{
		BasicDB bdb=new BasicDB();

		Properties p=new Properties();
		p.setProperty("Sky","Blue");
		p.setProperty("Ocean","Wet");

		bdb.setProperties(p);

		bdb.init();

		HashMap<String,String> fields=new HashMap<String,String>();
		fields.put("A","X");
		fields.put("B","Y");

		bdb.read("table","key",null,null);
		bdb.insert("table","key",fields);

		fields=new HashMap<String,String>();
		fields.put("C","Z");

		bdb.update("table","key",fields);

		bdb.delete("table","key");
	}*/
}
