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

public class combine_collections extends GenericUDF{

    ListObjectInspector listOI;
    StringObjectInspector sepaOI;

    // takes actual arguments and returns result
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2){
            throw new UDFArgumentLengthException("combine collections only takes 2 arguments: List<string>, string");
        }

        // 1. Check we received the right object types.
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)){
            throw new UDFArgumentException("first argument must be a list / array, " +
                    "second argument must be a string");
        }
        this.listOI = (ListObjectInspector) a;
        this.sepaOI = (StringObjectInspector) b;

        // 2. Check that the list contains lists
        if (!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)){
            throw new UDFArgumentException("first argument must be a list of strings");
        }

        // the return type of our function is a boolaen, so we provide the correct object inspector
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.
                writableStringObjectInspector);
//        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    // doesn't really matter, we can return anything, but should be a string representation of the function
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Set<String> inter_result = new HashSet<>();
        ArrayList<Text> final_result = new ArrayList<Text>();

        // get the list and string from the deferred objects using the object inspectors
        List<Text> textList = (List<Text>) this.listOI.getList(deferredObjects[0].get());
        String sepa = sepaOI.getPrimitiveJavaObject(deferredObjects[1].get());

        // check for nulls
        if (textList == null || sepa == null){
            return null;
        }

        for(Text s: textList){
            inter_result.addAll(Arrays.asList(s.toString().split(sepa)));
        }

        for(String s: inter_result){
            final_result.add(new Text(s));
        }
        return final_result;
    }

    // called once, before any evaluate() calls. You receive an array of object inspectors that represent the arguments
    // of the function. This is where you validate that the function is receiving the correct argument types, and the
    // correct number of arguments
    @Override
    public String getDisplayString(String[] strings) {
        return "combine_collections()";
    }
}
