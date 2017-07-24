package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateAggregateClause implements EvaluateEngine {
	private List<String> record;
	List<AggregateFunction> aggregates;
	private BufferedReader reader;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {

		aggregates = queryParameter.getAggregateFunctions();
		Map<String, Integer> header = queryParameter.getHeader();
		reader = getBufferedReader(queryParameter);
		// read first record and set to min, max, sum
		initializeAggregates(reader, aggregates, header);
		// find min, max, sum and count based on given aggregate function
		calclulateAggregates(reader, aggregates, header);

		ResultSet resultSet = new ResultSet();
		resultSet.setAggregateFunctions(aggregates);
		closeFile(reader);
		return resultSet;
	}

	private List<AggregateFunction> calclulateAggregates(BufferedReader reader, List<AggregateFunction> aggregates,
			Map<String, Integer> header) {
		String field;
		int currentValue = 0;
		int count =1;
		int sum=0;
		AggregateFunction currentAggregate = null;
		while ((record = getRecord(reader)) != null) {
			for (AggregateFunction aggregate : aggregates) {
				currentAggregate = aggregate;
				if (aggregate.getField().equals("*")) {
					field = "*";
				} else {
					field = record.get(header.get(aggregate.getField()));
				}

				// convert to integer if it contains all digits
				if (field.matches("\\d+")) {
					currentValue = Integer.parseInt(field);
				}
				switch (aggregate.getFunction()) {
				case "count":
					if (!field.isEmpty())
						aggregate.setResult(aggregate.getResult() + 1);
					count = aggregate.getResult();

					break;
				case "min":
					if (aggregate.getResult() > currentValue) {
						aggregate.setResult(currentValue);
					}
					break;
				case "max":
					if (aggregate.getResult() < currentValue) {
						aggregate.setResult(currentValue);
					}

					break;
				case "sum":
					aggregate.setResult(aggregate.getResult() + currentValue);
					sum = aggregate.getResult();
					break;
					
				case "avg":
					//not satisfied this logic. need to think
					aggregate.setResult((aggregate.getResult() + currentValue)/count++);
			
				}
				

			}
			
		}
		return aggregates;

	}

	/**
	 * Set count = 1 Set min, max, sum as first record value
	 * 
	 * @param reader
	 * @param aggregates
	 */
	private void initializeAggregates(BufferedReader reader, List<AggregateFunction> aggregates,
			Map<String, Integer> header) {
		List<String> record = getRecord(reader);
		for (AggregateFunction aggregate : aggregates) {
			switch (aggregate.getFunction()) {
			case "count":
				aggregate.setResult(1);
				break;
			case "min":
			case "max":
			case "sum":
				aggregate.setResult(Integer.parseInt(record.get(header.get(aggregate.getField()))));

			}
		}
	}

}
