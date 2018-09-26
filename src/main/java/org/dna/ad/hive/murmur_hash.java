package org.dna.ad.hive;

/**
 * Created by vincent.tan on 2017/04/05.
 */

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
/**
 * Murmur hash 2.0.
 *
 * The murmur hash is a relative fast hash function from
 * http://murmurhash.googlepages.com/ for platforms with efficient
 * multiplication.
 *
 * This is a re-implementation of the original C code plus some additional
 * features.
 *
 * Public domain.
 *
 * @author Viliam Holub
 * @version 1.0.2
 */
public class murmur_hash extends GenericUDF{


    StringObjectInspector rpOI;

    // takes actual arguments and returns result
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentLengthException("combine collections only takes 1 arguments: string");
        }

        // 1. Check we received the right object types.
        ObjectInspector a = objectInspectors[0];
        if (!(a instanceof StringObjectInspector)){
            throw new UDFArgumentException("first argument must be a list / array, " +
                    "second argument must be a string");
        }

        this.rpOI = (StringObjectInspector) a;

        // the return type of our function is a boolaen, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    // doesn't really matter, we can return anything, but should be a string representation of the function
    @Override
    public Object evaluate(GenericUDF.DeferredObject[] deferredObjects) throws HiveException {

        // get the list and string from the deferred objects using the object inspectors
//        String rp = rpOI.getPrimitiveJavaObject(deferredObjects[0].get());

        return new Text(Integer.toString(MurmurHash.hash32abs(rpOI.getPrimitiveJavaObject(deferredObjects[0].get()))));
    }

    // called once, before any evaluate() calls. You receive an array of object inspectors that represent the arguments
    // of the function. This is where you validate that the function is receiving the correct argument types, and the
    // correct number of arguments
    @Override
    public String getDisplayString(String[] strings) {
        return "murmur_hash()";
    }
}