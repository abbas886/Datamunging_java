package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunging.Query;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.QueryParameter;

public interface EvaluateEngine {
	
	public ResultSet evaluate(QueryParameter queryParameter);

	public default BufferedReader getBufferedReader(QueryParameter queryParameter) {
		try {
			
			BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()));
			// read header
			reader.readLine();
			return reader;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public default List<String> getRecord(BufferedReader reader) {
		try {
			String line;
			if((line =reader.readLine())!=null)
			{
				return	Arrays.asList(line.split(","));
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
	
	public default void closeFile(BufferedReader reader)
	{
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
