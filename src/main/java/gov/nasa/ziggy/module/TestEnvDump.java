package gov.nasa.ziggy.module;

import java.util.Map;

import org.apache.commons.exec.environment.EnvironmentUtils;

/**
 * @author Todd Klaus
 */
public class TestEnvDump {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Map<?, ?> env = EnvironmentUtils.getProcEnvironment();

        for (Object key : env.keySet()) {
            Object value = env.get(key);
            System.out.println(key + "=" + value);
        }
    }
}
