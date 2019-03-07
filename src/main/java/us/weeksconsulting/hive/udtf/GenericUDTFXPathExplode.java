package us.weeksconsulting.hive.udtf;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.stream.StreamSource;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.Text;

import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;

public class GenericUDTFXPathExplode extends GenericUDTF {

    private transient final StringObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;;
    private transient XdmValue rootNodes;
    private transient DocumentBuilder builder;
    private transient XPathSelector rootXPathSelector;
    private transient List<XPathSelector> colXPathSelectors;
    private transient int colCount;
    private transient XdmItem currentNode;
    private transient Object[] retCols;
    private transient String rootXPathExpression;
    private transient boolean debugEnabled;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 4 && argOIs.length != 5) {
            throw new UDFArgumentException("Invalid Arguments: 4 or 5 Arguments Expected");
        } else if (!(argOIs[1] instanceof StandardConstantMapObjectInspector)) {
            throw new UDFArgumentException("Argument 2 Should be a Map Constant");
        } else if (!(argOIs[2] instanceof WritableConstantStringObjectInspector)) {
            throw new UDFArgumentException("Argument 3 Should be a String Constant");
        } else if (!(argOIs[3] instanceof StandardConstantMapObjectInspector)) {
            throw new UDFArgumentException("Argument 4 Should be a Map Constant");
        } else if (argOIs.length == 5 && !(argOIs[4] instanceof WritableConstantBooleanObjectInspector)) {
            throw new UDFArgumentException("Argument 5 Should be a Boolean Constant");
        }

        Map<?, ?> namespaceMap = ((StandardConstantMapObjectInspector) argOIs[1]).getWritableConstantValue();
        rootXPathExpression = ((WritableConstantStringObjectInspector) argOIs[2]).getWritableConstantValue().toString();
        Map<?, ?> columnMap = ((StandardConstantMapObjectInspector) argOIs[3]).getWritableConstantValue();

        if (argOIs.length == 5) {
            debugEnabled = ((WritableConstantBooleanObjectInspector) argOIs[4]).getWritableConstantValue().get();
        } else {
            debugEnabled = false;
        }

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        Processor processor = new Processor(false);
        builder = processor.newDocumentBuilder();
        XPathCompiler xPathCompiler = processor.newXPathCompiler();
        colXPathSelectors = new ArrayList<>();

        for (Entry<?, ?> entry : namespaceMap.entrySet()) {
            Text prefix = (Text) entry.getKey();
            Text uri = (Text) entry.getValue();
            xPathCompiler.declareNamespace(prefix.toString(), uri.toString());
        }

        try {
            rootXPathSelector = xPathCompiler.compile(rootXPathExpression).load();
        } catch (Exception e) {
            throw new UDFArgumentException(new Throwable("Failed to Parse XPath for Root", e));
        }

        for (Entry<?, ?> colEntry : columnMap.entrySet()) {
            Text colName = (Text) colEntry.getKey();
            Text colXPath = (Text) colEntry.getValue();
            fieldNames.add(colName.toString().toLowerCase());
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            try {
                colXPathSelectors.add(xPathCompiler.compile(colXPath.toString()).load());
            } catch (Exception e) {
                throw new UDFArgumentException(
                        new Throwable("Failed to Parse XPath for Column <" + colName.toString() + ">", e));
            }
        }

        if (debugEnabled) {
            fieldNames.add("debug");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        colCount = fieldNames.size();
        retCols = new Object[colCount];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String xml = stringOI.getPrimitiveJavaObject(args[0]);
        try {
            XdmNode rootNode = builder.build(new StreamSource(new StringReader(xml)));
            rootXPathSelector.setContextItem(rootNode);
            rootNodes = rootXPathSelector.evaluate();

            for (int r = 0; r < rootNodes.size(); r++) {
                currentNode = rootNodes.itemAt(r);
                for (int c = 0; c < colCount; c++) {
                    if (!debugEnabled || c != colCount - 1) {
                        XPathSelector colXPathSelector = colXPathSelectors.get(c);
                        colXPathSelector.setContextItem(currentNode);
                        XdmItem colItem = colXPathSelector.evaluateSingle();
                        String colValue = colItem == null ? "" : colItem.getStringValue();
                        retCols[c] = colValue;
                    } else {
                        retCols[c] = null;
                    }
                }
                forward(retCols);
            }

        } catch (SaxonApiException e) {
            // This should almost always be something wrong with the XML Document
            // Return all nulls here because otherwise there is no way to identify which
            // document is bad.
            for (int c = 0; c < colCount; c++) {
                if (!debugEnabled || c != colCount - 1) {
                    retCols[c] = null;
                } else {
                    retCols[c] = e.toString();
                }
            }
            forward(retCols);
        }
    }

    @Override
    public void close() throws HiveException {
    }

}