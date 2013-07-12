import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.cascading.splunk.SplunkScheme;
import com.yolodata.tbana.cascading.splunk.SplunkTap;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;

import java.util.Properties;

public class SplunkSchemeExample {

    private static final String PATH_TO_OUTPUT = "output/splunk-scheme-example";

    public static void main(String [] args) {

        Properties properties = new Properties();

        properties.put(SplunkConf.SPLUNK_USERNAME, "admin");
        properties.put(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        properties.put(SplunkConf.SPLUNK_HOST, "localhost");
        properties.put(SplunkConf.SPLUNK_PORT, "9050");

        SplunkDataQuery splunkSearch = new SplunkDataQuery("-12m","now");
        SplunkScheme inputScheme = new SplunkScheme(splunkSearch);
        SplunkTap input = new SplunkTap(inputScheme);

        TextLine outputScheme = new TextLine();
        Hfs output = new Hfs( outputScheme, PATH_TO_OUTPUT, SinkMode.REPLACE );

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();
    }
}
