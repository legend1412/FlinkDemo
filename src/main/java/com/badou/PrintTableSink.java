package com.badou;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class PrintTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

private String target;
private PrintTableSinkFunction sinkFunction;

public PrintTableSink(String target) {
this.target = target;

/**
 * 重点！！！
 *
 * PrintTableSinkFunction 是一个自定义的 SinkFunction
 * 描述了当接收到一条数据时，该如何 sink 的具体逻辑
 */
this.sinkFunction = new PrintTableSinkFunction(target);
}

/**
 * 添加当流被消费时的 sink 逻辑
 */
@Override
public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
return dataStream.addSink(this.sinkFunction);
}

/**
 * 对 "流" 添加 sink 逻辑（单条数据）
 */
@Override
public void emitDataStream(DataStream<Row> dataStream) {
dataStream.addSink(this.sinkFunction);
}

/**
 * 对 "批" 添加 sink 逻辑（多条数据）
 */
@Override
public void emitDataSet(DataSet<Row> dataSet) {

try {

List<Row> elements = dataSet.collect();
for (Iterator<Row> it = elements.iterator(); it.hasNext(); ) {
Row row = it.next();
this.sinkFunction.invoke(row);
}

dataSet.print();
} catch (Exception e) {
e.printStackTrace();
}
}


private String[] fieldNames;
private TypeInformation<?>[] fieldTypes;

/**
 * 当 StreamTableEnvironment.registerTableSink() 时，会通过此方法完成 TableSink 对象的创建。
 *
 * @param strings          字段名列表
 * @param typeInformations 字段类型列表
 * @return
 */
@Override
public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
PrintTableSink sink = new PrintTableSink(target);
sink.fieldNames = strings;
sink.fieldTypes = typeInformations;

return sink;
}

/**
 * 表的字段列表
 */
@Override
public String[] getFieldNames() {
return fieldNames;
}

/**
 * 表字段的数据类型
 */
@Override
public TypeInformation<?>[] getFieldTypes() {
return fieldTypes;
}

/**
 * 表字段类型的描述信息
 */
@Override
public TypeInformation<Row> getOutputType() {
return Types.ROW_NAMED(fieldNames, fieldTypes);
}


/**
 * 这里定义了当接收到一条数据时，该如何 sink 的具体逻辑
 */
public static class PrintTableSinkFunction implements SinkFunction<Row> {
private static Logger LOG = LoggerFactory.getLogger(PrintTableSink.class);
private String target;

public PrintTableSinkFunction(String target) {
this.target = target;
}

@Override
public void invoke(Row row, Context context) throws Exception {
switch (target) {
case "Console":
System.out.println(row);
break;
case "Logger":
LOG.info(row.toString());
break;
default:
}
}

@Override
public void invoke(Row value) throws Exception {
invoke(value, null);
}
}

}