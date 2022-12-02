package gov.nasa.ziggy.services.config;

import java.util.Properties;

/**
 * @author Todd Klaus
 */
public class DumpSystemProperties {
    /**
     * @param args
     */
    public static void main(String[] args) {
        Properties props = System.getProperties();
        for (Object key : props.keySet()) {
            System.out.println(key + "=" + props.getProperty((String) key));
        }
    }
}
