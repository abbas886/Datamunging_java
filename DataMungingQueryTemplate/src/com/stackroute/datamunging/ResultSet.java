package com.stackroute.datamunging;

import java.util.List;
import java.util.Map;

public class ResultSet {
	
	private List<List<String>> result;
	
	private List<AggregateResult> aggregateResults;
	
	public List<List<String>> getResult() {
		return result;
	}

	public void setResult(List<List<String>> result) {
		this.result = result;
	}

	public List<AggregateResult> getAggregateResults() {
		return aggregateResults;
	}

	public void setAggregateResults(List<AggregateResult> aggregateResults) {
		this.aggregateResults = aggregateResults;
	}

	

}

