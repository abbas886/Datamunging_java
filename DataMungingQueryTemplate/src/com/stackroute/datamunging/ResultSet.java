package com.stackroute.datamunging;

import java.util.List;
import java.util.Map;

import com.stackroute.query.parser.AggregateFunction;

public class ResultSet {
	
	private List<List<String>> result;
	
	private List<AggregateFunction>  aggregateFunctions;
	
	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public List<List<String>> getResult() {
		return result;
	}

	public void setResult(List<List<String>> result) {
		this.result = result;
	}

	

}

