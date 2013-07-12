import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.cascading.splunk.ShuttlCsv;

import java.util.Properties;

public class ShuttlCsvExample {

    private static final String PATH_TO_SHUTTL_ARCHIVE = "shuttl";
    private static final String PATH_TO_OUTPUT = "output/shuttl-example";

    public static void main(String [] args) {

        ShuttlCsv csv = new ShuttlCsv();
        Hfs input = new Hfs(csv,PATH_TO_SHUTTL_ARCHIVE);

        TextLine outputScheme = new TextLine();
        Hfs output = new Hfs(outputScheme, PATH_TO_OUTPUT);
        Pipe pipe = new Pipe( "test" );

        Properties properties = new Properties();
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();
    }
}
