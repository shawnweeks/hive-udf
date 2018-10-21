package us.weeksconsulting.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class GenericUDFToTimestamp extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFToTimestamp.class.getName());

    private transient WritableStringObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    private transient final TimestampWritable output = new TimestampWritable();

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("to_timestamp() requires two arguments, got " + arguments.length);
        } else if (!(arguments[0] instanceof WritableStringObjectInspector)) {
            throw new UDFArgumentException("to_timestamp requires a string for the first argument, got " + arguments[0].getTypeName());
        } else if (!(arguments[1] instanceof WritableStringObjectInspector)) {
            throw new UDFArgumentException("to_timestamp requires a string for the second argument, got " + arguments[1].getTypeName());
        }
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String timeStampString = inputOI.getPrimitiveJavaObject(arguments[0].get());
        String timeStampFormat = inputOI.getPrimitiveJavaObject(arguments[1].get());
        DateTimeFormatter formatter = DateTimeFormat.forPattern(timeStampFormat);
        LocalDateTime parsedDate = LocalDateTime.parse(timeStampString, formatter);
        Timestamp timestamp = new Timestamp(parsedDate.toDateTime().getMillis());
        output.set(timestamp);
        return output;
    }

    public String getDisplayString(String[] children) {
        return getStandardDisplayString("to_timestamp", children);
    }
}
