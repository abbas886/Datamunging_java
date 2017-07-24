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

public class EvaluateOrderByClause implements EvaluateEngine {
	private ResultSet resultSet;
	private List<String> record;
	private FilterHandler filterHandler;
	private BufferedReader reader;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		resultSet = new ResultSet();
		filterHandler = new FilterHandler();

		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();
		List<String> orderByFields = queryParameter.getOrderByFields();
		String orderByField = orderByFields.get(0); // presently working with
													// only one order by clause
		int orderByFieldIndex = queryParameter.getHeader().get(orderByField);

		boolean requiredAllFields = selectedFields.get(0).equals("*");
		reader = getBufferedReader(queryParameter);
		// get first record
		result.add(getRecord(reader));
		while ((record = getRecord(reader)) != null) {
			if (!requiredAllFields) {
				record = filterHandler.filterFields(queryParameter, record);
			}
			insertionSort(result, record, orderByFieldIndex);

		}
		resultSet.setResult(result);

		return resultSet;
	}

	/**
	 * 
	 * @param result
	 * @param record
	 * @param orderByFieldIndex
	 */
	private void insertionSort(List<List<String>> result, List<String> record, int orderByFieldIndex) {

		for (int position = 0; position < result.size(); position++) {
			try {
				// if it is integer field
				if (Integer.parseInt(result.get(position).get(orderByFieldIndex)) < Integer
						.parseInt(record.get(orderByFieldIndex))) {
					result.add(position, record);
					break;
				}
			} catch (NumberFormatException e) {
				if (result.get(position).get(orderByFieldIndex).compareTo(record.get(orderByFieldIndex)) < 0) {
					result.add(position, record);
					break;
				}
			}
		}

	}

}
