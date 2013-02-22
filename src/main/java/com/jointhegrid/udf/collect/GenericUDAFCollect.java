/*
Copyright 2012 jointhegrid.com Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.jointhegrid.udf.collect;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "collect", value = "_FUNC_(x) - Returns a list of objects. CAUTION will easily OOM on large datasets" )
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFCollect.class.getName());

  public GenericUDAFCollect() {
  }

 @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    return new GenericUDAFMkListEvaluator();
  }

   public static class GenericUDAFMkListEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private ObjectInspector inputOI;
    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
    // of objs)
    private StandardListObjectInspector loi;

    private StandardListObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      // init output object inspectors
      // The output of a partial aggregation is a list
      if (m == Mode.PARTIAL1) {
        inputOI = (ObjectInspector) parameters[0];
        return ObjectInspectorFactory
            .getStandardListObjectInspector((ObjectInspector) ObjectInspectorUtils
                .getStandardObjectInspector(inputOI));
      } else {
        if (!(parameters[0] instanceof StandardListObjectInspector)) {
          //no map aggregation.
          inputOI = (ObjectInspector)  ObjectInspectorUtils
          .getStandardObjectInspector(parameters[0]);
          return (StandardListObjectInspector) ObjectInspectorFactory
              .getStandardListObjectInspector(inputOI);
        } else {
          internalMergeOI = (StandardListObjectInspector) parameters[0];
          inputOI = (ObjectInspector) internalMergeOI.getListElementObjectInspector();
          loi = (StandardListObjectInspector) ObjectInspectorUtils
                  .getStandardObjectInspector(internalMergeOI);
          return loi;
        }
      }
    }

    static class MkArrayAggregationBuffer implements AggregationBuffer {
      List<Object> container;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((MkArrayAggregationBuffer) agg).container = new ArrayList<Object>();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
      reset(ret);
      return ret;
    }

    //mapside
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      Object p = parameters[0];

      if (p != null) {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        putIntoList(p, myagg);
      }
    }

    //mapside
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
      for(Object i : partialResult) {
        putIntoList(i, myagg);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }

    private void putIntoList(Object p, MkArrayAggregationBuffer myagg) {
      Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,this.inputOI);
      myagg.container.add(pCopy);
    }
  }

}