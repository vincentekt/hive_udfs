/**
 * Created by vincent.tan on 3/4/17.
 */

package org.dna.ad.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.*;

public class count_collection extends GenericUDF{

    ListObjectInspector listOI;
    StringObjectInspector targetOI;

    // Takes actual arguments and returns result
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2){
            throw new UDFArgumentLengthException("Combine collections only takes 2 arguments: List<string>, string");
        }

        // 1. Check we received the right object types.
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)){
            throw new UDFArgumentException("First argument must be a list / array, " +
                    "second argument must be a string");
        }
        this.listOI = (ListObjectInspector) a;
        this.targetOI = (StringObjectInspector) b;

        // 2. Check that the list contains string
        if (!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)){
            throw new UDFArgumentException("First argument must be a list of strings");
        }

        // The return type of our function is a boolaen, so we provide the correct object inspector
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.
                writableStringObjectInspector);
//        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Set<String> inter_result = new HashSet<>();

        // get the list and string from the deferred objects using the object inspectors
        List<Text> textList = (List<Text>) this.listOI.getList(deferredObjects[0].get());
        String target = targetOI.getPrimitiveJavaObject(deferredObjects[1].get());

        // check for nulls
        if (textList == null || target == null){
            return null;
        }

        for(Text s: textList){
            inter_result.addAll(Arrays.asList(s.toString()));
        }

        int occurrences = Collections.frequency(inter_result, target);
        return occurrences;
    }

    // called once, before any evaluate() calls. You receive an array of object inspectors that represent the arguments
    // of the function. This is where you validate that the function is receiving the correct argument types, and the
    // correct number of arguments
    @Override
    public String getDisplayString(String[] strings) {
        return "count_collection()";
    }
}
