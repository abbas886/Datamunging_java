package com.stackroute.datamunging.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stackroute.query.parser.QueryParameter;
import com.stackroute.query.parser.Restriction;

public class FilterHandler {
	
	Map<String, Integer> header;
	public List<String> filterFields(QueryParameter queryParameter, List<String> record) {
		List<String> selectedFields = queryParameter.getFields();
		header = queryParameter.getHeader();
		int fieldPosition;
		List<String> filteredRecord = new ArrayList<String>();
		for (String field : selectedFields) {
			fieldPosition =header.get(field);
			filteredRecord.add(record.get(fieldPosition));
			//record.remove(fieldPosition);
		}
		return filteredRecord;

	}

	public boolean isRequiredRecord(QueryParameter queryParameter, List<String> record) {
		header = queryParameter.getHeader();
		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<String> logicationOperators = queryParameter.getLogicalOperators();
		Map<String, Integer> header = queryParameter.getHeader();
		boolean flag = false;
		int expressionPosotion = 0;
		if (restrictions != null && !restrictions.isEmpty()) {
			// evaluate first expression
			Restriction restriction = restrictions.get(expressionPosotion);
			flag = evaluateRelationalExpression(record, restriction);

			// evaluate remaining expressions
			for (String logicationOperator : logicationOperators) {
				expressionPosotion++;
				switch (logicationOperator) {
				case "and":
					restriction = restrictions.get(expressionPosotion);
					flag = flag && evaluateRelationalExpression(record, restriction);
					break;
				case "or":
					restriction = restrictions.get(expressionPosotion);
					flag = flag || evaluateRelationalExpression(record, restriction);
					break;
				case "not":
					restriction = restrictions.get(expressionPosotion);
					flag = flag && !evaluateRelationalExpression(record, restriction);
				}
			}

		}
		return flag;

	}

	/**/

	private boolean evaluateRelationalExpression(List<String> record, Restriction restriction) {
		boolean flag = true;
		String condition = restriction.getCondition();
		String propertyName = restriction.getPropertyName();

		String whereConditionValue = restriction.getPropertyValue();

		int propertPosition = header.get(propertyName);
		String recordValue = record.get(propertPosition);

		switch (condition.trim()) {
		case "=":
			if (whereConditionValue.equals(recordValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}
			break;
		case "!=":

			if (!whereConditionValue.equals(recordValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}
			break;

		case ">":

			if (Double.parseDouble(recordValue) > Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

			break;

		case "<":

			if (Double.parseDouble(recordValue) < Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

		}

		return flag;
	}

}
