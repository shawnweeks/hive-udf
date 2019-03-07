package us.weeksconsulting.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.net.URL;
import java.net.URLClassLoader;

public class UDFPrintClassPath extends UDF {

    public Text evaluate() {
        StringBuilder sb = new StringBuilder();
        URLClassLoader cl = (URLClassLoader) this.getClass().getClassLoader();
        for (URL url : cl.getURLs()) {
            sb.append(url.toString()).append(",");
        }

        return new Text(sb.toString());
    }
}
