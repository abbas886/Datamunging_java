package com.stackroute.datamunging.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunging.Query;
import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;

public class DataMungingTestCase {

	private static Query query;

	private String queryString;

	@BeforeClass
	public static void init() {
	
			query = new Query();
	}

	@Test
	public void getAllRecords() {
		queryString = "select * from D:/employee.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionEqual() {
		queryString = "select * from D:/employee.csv where department = 'Data Munging'";

		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionNotEqual() {
		queryString = "select * from D:/employee.csv where department != 'Data Munging'";

		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionEqualAndNotEqual() {
		queryString = "select * from D:/employee.csv where department = 'Data Munging' and location != 'Bombay'";

		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionEqualAndEqual() {
		queryString = "select * from D:/employee.csv where department = 'Data Munging'  and  location = 'Bombay'";

		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionEqualOrEqual() {
		queryString = "select * from D:/employee.csv where department = 'Data Munging'  or  department = 'Hobes'";

		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionGreaterThan() {
		queryString = "select * from D:/employee.csv where age > 70";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void restrictionLessThanAndEqual() {
		queryString = "select * from D:/employee.csv where age < 40 and location != 'Bangalore'";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void getSelectedFields() {
		queryString = "select id, name, salary from D:/employee.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}

	@Test
	public void orderByField() {
		queryString = "select * from D:/employee.csv order by salary";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}
	
	@Test
	public void aggregateFunctionsCountTestCase() {
		queryString = "select count(*) from D:/employee.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayAggregateResult(queryString, records.getAggregateFunctions());
	}
	
	@Test
	public void aggregateFunctionsSumTestCase() {
		queryString = "select sum(salary) from D:/employee.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayAggregateResult(queryString, records.getAggregateFunctions());
	}
	
	@Test
	public void aggregateFunctionsMinMaxAvgTestCase() {
		queryString = "select min(salary), max(age), avg(salary) from D:/employee.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayAggregateResult(queryString, records.getAggregateFunctions());
	}
	
	
	private void displayAggregateResult(String queryString2, List<AggregateFunction> aggregateFunctions) {
		System.out.println("\nGiven Query : " + queryString);
		aggregateFunctions.forEach(aggregate->{
			System.out.print(aggregate.getFunction()+"(");
			System.out.print(aggregate.getField()+"): ");
		System.out.println(aggregate.getResult());});		
	}

	private void displayRecords(String queryString, ResultSet records) {
		System.out.println("\nGiven Query : " + queryString);
		records.getResult().forEach(System.out::println);

	}

}
