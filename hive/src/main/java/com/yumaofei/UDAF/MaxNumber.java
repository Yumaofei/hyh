package com.yumaofei.UDAF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class MaxNumber extends AbstractGenericUDAFResolver {
    static final Log LOG =  LogFactory.getLog(GenericUDAFSum.class.getName());

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
                return new GenericUDAFSum.GenericUDAFSumLong();
            case FLOAT:
            case DOUBLE:
            case STRING:
                return new GenericUDAFSum.GenericUDAFSumDouble();
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but"+info[0].getTypeName()+"is passed.");
        }
    }

    public static class max extends GenericUDAFEvaluator{
        private PrimitiveObjectInspector inputOI;
        private LongWritable result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length ==1);
            super.init(m, parameters);
            result = new LongWritable(0);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        static class MaxOfValuePartial implements AggregationBuffer {
            boolean aBoolean;
            Long aLong;

            public boolean empty(){
                return aLong.equals(null);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MaxOfValuePartial result = new MaxOfValuePartial();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            MaxOfValuePartial myagg =(MaxOfValuePartial) aggregationBuffer;
            myagg.aBoolean = true;
            myagg.aLong = 0L;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            assert (objects.length == 1);
            merge(aggregationBuffer,objects[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o != null){
                MaxOfValuePartial myagg =(MaxOfValuePartial) aggregationBuffer;
                myagg.aLong =  Math.max(myagg.aLong, PrimitiveObjectInspectorUtils.getLong(o,inputOI));
                myagg.aBoolean = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            MaxOfValuePartial myagg = (MaxOfValuePartial) aggregationBuffer;
            if (myagg.empty())
                return null;
            result.set(myagg.aLong);
            return result;
        }
    }
}

