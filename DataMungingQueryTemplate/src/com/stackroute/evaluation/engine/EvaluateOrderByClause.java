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

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		resultSet = new ResultSet();
		filterHandler = new FilterHandler();

		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();
		List<String> orderByFields = queryParameter.getOrderByFields();
		String orderByField = orderByFields.get(0); // presently working with
													// only one order clause
		int orderByFieldIndex = queryParameter.getHeader().get(orderByField);
		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line = reader.readLine()) != null) {
				record = Arrays.asList(line.split(","));
				if (!selectedFields.get(0).equals("*")) {
					record = filterHandler.filterFields(queryParameter, record);
				}
				if(result.isEmpty())
				{
					result.add(record);  //add first record
					continue;
				}

				insertionSort(result, record, orderByFieldIndex);

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
		for(int position =0; position<result.size();position++)
		{
			try {
				//if it is integer field
				if(Integer.parseInt(result.get(position).get(orderByFieldIndex))< Integer.parseInt(record.get(orderByFieldIndex)))
				{
					result.add(position, record);
					break;
				}
			} catch (NumberFormatException e) {
				if(result.get(position).get(orderByFieldIndex).compareTo(record.get(orderByFieldIndex))<0)
				{
					result.add(position, record);
					break;
				}
			}
		}

	}

}
