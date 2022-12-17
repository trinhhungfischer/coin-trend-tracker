package com.trinhhungfischer.cointrendy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
public class PropertyFileReader {
    private static final Logger logger = Logger.getLogger(PropertyFileReader.class);
    private static Properties prop = new Properties();
    public static Properties readPropertyFile() throws Exception {
        if (prop.isEmpty()) {
            InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream("iot-kafka.properties");
            try {
                prop.load(input);
            } catch (IOException ex) {
                logger.error(ex);
                throw ex;
            } finally {
                if (input != null) {
                    input.close();
                }
            }
        }
        return prop;
    }
}
