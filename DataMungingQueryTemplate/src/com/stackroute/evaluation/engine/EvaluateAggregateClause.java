package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateAggregateClause implements EvaluateEngine {
	private List<String> record;

		@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
			
			int count =0,sum=0,max,min,avg;
			
			//AggregateFunction aggregateFunction = new AggregateFunction();
		     String aggregateFunction;
			 String aggregateField;
			try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read header
			reader.readLine().split(",");
			// read the remaining records
		
			List<AggregateFunction> aggregates = queryParameter.getAggregateFunctions();
			
			
			while ((reader.readLine()) != null) {
				record = Arrays.asList(reader.readLine().split(","));
				for(AggregateFunction aggregate:aggregates)
				{
				
					aggregateFunction = aggregate.getFunction();
					aggregateField = aggregate.getField();
		            int aggregateFieldIndex = aggregate.getAggregateFieldIndex();
					switch (aggregateFunction) {
					case "count":
						         count = count + Integer.parseInt(record.get(aggregateFieldIndex));
						break;
					case "min":

						break;
					case "max":

						break;
					case "avg":

						break;
					case "sum":

						break;

					}

				}

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		return null;
	}

	
	
}
