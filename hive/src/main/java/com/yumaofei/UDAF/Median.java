package com.yumaofei.UDAF;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.List;

public class Median extends AbstractGenericUDAFResolver {
    //
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length!=1)
            throw new UDFArgumentTypeException(info.length-1,"Exactly one argument is expected.");
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE)
            throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but"+info[0].getTypeName()+"is passed.");
        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()){
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                return new IntEvaluator();
            case FLOAT:
            case DOUBLE:
            case STRING:
                return new IntEvaluator();
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but"+info[0].getTypeName()+"is passed.");
        }
    }

    public static class IntEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private DoubleWritable result;

        static class MedianOfValuePartial implements AggregationBuffer {
            List<Object> container = Lists.newArrayList();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianOfValuePartial medianOfValuePartial = new MedianOfValuePartial();
            reset(medianOfValuePartial);
            return medianOfValuePartial;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length ==1);
            super.init(m, parameters);
            result = new DoubleWritable(0);
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MedianOfValuePartial) agg).container.clear();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg,parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return null;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianOfValuePartial myAgg = (MedianOfValuePartial) agg;
            List<Object> partialResult = (List<Object>)partial;
            for (Object ob : partialResult) {
                myAgg.container.add(ObjectInspectorUtils.copyToStandardObject(ob, this.inputOI));
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return null;
        }
    }
}
