package kdp.utils;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * Functions: public functions
 *
 * @author chenlifei
 * @date 2017/12/18
 */
public class Functions {
    private static final Logger logger = LoggerFactory.getLogger(Functions.class);

    public static String readJsonFile(String filename) throws ParseException, IOException {
        org.json.simple.JSONObject conf = new org.json.simple.JSONObject();
        JSONParser jsonParser = new JSONParser();

        InputStream resourceAsStream = Functions.class.getClassLoader().getResourceAsStream(filename + ".json");
        conf = (org.json.simple.JSONObject) jsonParser.parse(new InputStreamReader(resourceAsStream, "UTF-8"));

        return conf.toString();
    }
}
