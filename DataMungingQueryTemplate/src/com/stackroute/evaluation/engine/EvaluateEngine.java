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
	
	
	 default List<String> getHeader(String file)
	 {
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {

					String line;
					if ((line = br.readLine()) != null) {
						return Arrays.asList(line.split(","));
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			return null;

	 }

}
