package com.trinhhungfischer.cointrendy.common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {
    private static final Logger logger = Logger.getLogger(PropertyFileReader.class);

    private static Properties prop = new Properties();

    public static Properties readPropertyFile(String file) throws Exception {
        if (prop.isEmpty()) {
            InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream(file);
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
