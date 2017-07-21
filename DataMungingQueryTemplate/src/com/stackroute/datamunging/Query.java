package com.stackroute.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stackroute.evaluation.engine.EvaluateAggregateClause;
import com.stackroute.evaluation.engine.EvaluateEngine;
import com.stackroute.evaluation.engine.EvaluateGroupeByClause;
import com.stackroute.evaluation.engine.EvaluateOrderByClause;
import com.stackroute.evaluation.engine.EvaluateSimpleQuery;
import com.stackroute.evaluation.engine.EvaluateWhereClauseQuery;
import com.stackroute.query.parser.QueryParameter;
import com.stackroute.query.parser.QueryParser;


public class Query {

	private ResultSet resultSet;
	
	private QueryParameter queryParameter;

	private QueryParser queryParser;
	
	private EvaluateEngine evaluateEngine = null;
	
	private String file;
	
	private Map<String, Integer> header;
	


	public ResultSet executeQuery(String queryString) {
		queryParser = new QueryParser();
		queryParameter =queryParser.parseQuery(queryString);
		resultSet = evaluateQuery();

		return resultSet;
	}

	private ResultSet evaluateQuery() {
		
		setHeader();

		switch (queryParameter.getQUERY_TYPE()) {
		
		case "SIMPLE_QUERY":
			evaluateEngine = new EvaluateSimpleQuery();
			break;
		
		case "WHERE_CLAUSE_QUERY":
			evaluateEngine = new EvaluateWhereClauseQuery();
			break;
	
		case "ORDER_BY_QUERY":
			evaluateEngine = new EvaluateOrderByClause();
			break;
			
		case "GROUPE_BY_QUERY":
			evaluateEngine = new EvaluateGroupeByClause();
			break;
			
		case "GROUPE_BY_ORDER_BY_QUERY":
			evaluateEngine = new EvaluateGroupeByClause();
			break;
	
			
		case "AGGREGATE_QUERY":
			evaluateEngine = new EvaluateAggregateClause();
		
			
		}
		
		return evaluateEngine.evaluate(queryParameter);
	}

	
	public void setHeader()
	{
		header = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read the header record
			List<String> record = Arrays.asList(reader.readLine().split(","));
			int columnSize=record.size();
			for(int columnIndex = 0; columnIndex<columnSize ;columnIndex++)
			{
				header.put(record.get(columnIndex), columnIndex);
			}
			
		queryParameter.setHeader(header);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}

	
	
}
