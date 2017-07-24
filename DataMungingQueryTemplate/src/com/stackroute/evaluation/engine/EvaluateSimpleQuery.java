package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunging.Query;
import com.stackroute.datamunging.ResultSet;
import com.stackroute.datamunging.util.FilterHandler;
import com.stackroute.query.parser.QueryParameter;
import com.stackroute.query.parser.Restriction;

public class EvaluateSimpleQuery implements EvaluateEngine {
	private ResultSet resultSet;
	private List<String> record;
	private FilterHandler filterHandler;

	private BufferedReader reader;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		filterHandler = new FilterHandler();
		resultSet = new ResultSet();
		List<String> selectedFields = queryParameter.getFields();
		List<List<String>> result = new ArrayList<List<String>>();
		reader = getBufferedReader(queryParameter);
		while ((record = getRecord(reader)) != null) {
			if (!selectedFields.get(0).equals("*")) {
				record = filterHandler.filterFields(queryParameter, record);
			}

			result.add(record);

		}

		resultSet.setResult(result);
		closeFile(reader);
		return resultSet;
	}

	/*
	 * @Override public ResultSet evaluate(QueryParameter queryParameter) {
	 * resultSet = new ResultSet(); filterHandler=new FilterHandler();
	 * 
	 * List<List<String>> result = new ArrayList<List<String>>(); List<String>
	 * selectedFields = queryParameter.getFields();
	 * 
	 * try (BufferedReader reader = new BufferedReader(new
	 * FileReader(queryParameter.getFile()))) { //read header
	 * reader.readLine().split(","); String line; // read the remaining records
	 * while ((line =reader.readLine()) != null) { record =
	 * Arrays.asList(line.split(",")); if(!selectedFields.get(0).equals("*")) {
	 * record=filterHandler.filterFields(queryParameter,record); }
	 * 
	 * result.add(record);
	 * 
	 * }
	 * 
	 * } catch (Exception e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } resultSet.setResult(result); return resultSet; }
	 */

}
