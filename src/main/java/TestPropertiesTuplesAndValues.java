import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.types.IntValue;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;

/**
 * Created by niklas on 22.11.16.
 */
public class TestPropertiesTuplesAndValues {

  private static final int ELEMENT_NUMBER = 4000000;

  @Test
  public void testInteger() throws Exception {
    ArrayList<Integer> tupleList = new ArrayList<>();
    for (int i = 0; i < ELEMENT_NUMBER; i++) {
      tupleList.add(1);
    }
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<Integer> tupleDataSet = env.fromCollection(tupleList);
    long time = System.currentTimeMillis();
    tupleDataSet = tupleDataSet.reduce((ReduceFunction<Integer>) (i1, i2) -> i1 + i2);
    int tupleResult = tupleDataSet.collect().get(0);
    time = System.currentTimeMillis() - time;
    System.out.println(tupleResult + " " + time);
  }

  @Test
  public void testIntegerTuple() throws Exception {
    ArrayList<Tuple1<Integer>> tupleList = new ArrayList<>();
    for (int i = 0; i < ELEMENT_NUMBER; i++) {
      tupleList.add(new Tuple1<>(1));
    }
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<Tuple1<Integer>> tupleDataSet = env.fromCollection(tupleList);
    long time = System.currentTimeMillis();
    tupleDataSet = tupleDataSet.sum(0);
    int tupleResult = tupleDataSet.collect().get(0).f0;
    time = System.currentTimeMillis() - time;
    System.out.println(tupleResult + " " + time);
  }

  @Test
  public void testIntValues() throws Exception {
    ArrayList<Tuple1<IntValue>> tupleList = new ArrayList<>();
    for (int i = 0; i < ELEMENT_NUMBER; i++) {
      tupleList.add(new Tuple1<>(new IntValue(1)));
    }
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<Tuple1<IntValue>> tupleDataSet = env.fromCollection(tupleList);
    long time = System.currentTimeMillis();
    tupleDataSet = tupleDataSet.sum(0);
    int tupleResult = tupleDataSet.collect().get(0).f0.getValue();
    time = System.currentTimeMillis() - time;
    System.out.println(tupleResult + " " + time);
  }

  @Test
  public void testProperties() throws Exception {

    ArrayList<PropertyValue> propertyList = new ArrayList<>();
    for (int i = 0; i < ELEMENT_NUMBER; i++) {
      propertyList.add(PropertyValue.create(1));
    }
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<PropertyValue> propertyDataSet = env.fromCollection(propertyList);
    long time = System.currentTimeMillis();
    propertyDataSet = propertyDataSet.reduce(new PropertyReducer());
    int propertyResult = propertyDataSet.collect().get(0).getInt();
    time = System.currentTimeMillis() - time;
    System.out.println(propertyResult + " " + time);
  }

  @Test
  public void testPropertyTuples() throws Exception {
    ArrayList<Tuple1<PropertyValue>> propertyList = new ArrayList<>();
    for (int i = 0; i < ELEMENT_NUMBER; i++) {
      propertyList.add(new Tuple1<>(PropertyValue.create(1)));
    }
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<Tuple1<PropertyValue>> propertyDataSet = env.fromCollection(propertyList);
    long time = System.currentTimeMillis();
    propertyDataSet = propertyDataSet.reduce(new PropertyTupleReducer());
    int propertyResult = propertyDataSet.collect().get(0).f0.getInt();
    time = System.currentTimeMillis() - time;
    System.out.println(propertyResult + " " + time);
  }

  private class IntValueReducer implements ReduceFunction<Tuple1<IntValue>> {

    @Override
    public Tuple1<IntValue> reduce(Tuple1<IntValue> t1, Tuple1<IntValue> t2) throws Exception {
      t1.setField(new IntValue(t1.f0.getValue() + t2.f0.getValue()), 0);
      return t1;
    }
  }

  private class PropertyReducer implements ReduceFunction<PropertyValue> {

    @Override
    public PropertyValue reduce(PropertyValue p1, PropertyValue p2) throws Exception {
      p1.setInt(p1.getInt() + p2.getInt());
      return p1;
    }
  }

  private class PropertyTupleReducer implements ReduceFunction<Tuple1<PropertyValue>> {

    @Override
    public Tuple1<PropertyValue> reduce(Tuple1<PropertyValue> p1, Tuple1<PropertyValue> p2) throws
      Exception {
      p1.f0.setInt(p1.f0.getInt() + p2.f0.getInt());
      return p1;
    }
  }

  private class TupleReducer implements ReduceFunction<Tuple1<Integer>> {

    @Override
    public Tuple1<Integer> reduce(Tuple1<Integer> t1, Tuple1<Integer> t2) throws Exception {
      t1.setField(t1.f0 + t2.f0, 0);
      return t1;
    }
  }
}
