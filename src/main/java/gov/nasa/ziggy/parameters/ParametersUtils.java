package gov.nasa.ziggy.parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Utility methods for {@link Parameters} classes
 *
 * @author Todd Klaus
 */
public class ParametersUtils {
    private static final Logger log = LoggerFactory.getLogger(ParametersUtils.class);

    /** Singleton pattern, all methods are static */
    private ParametersUtils() {
    }

    /**
     * Populate the specified {@link Parameters} class with values from the specified file. The file
     * is assumed to be in .properties format (see {@link Properties}.load for format details.
     *
     * @param file
     * @param parametersClass
     * @throws IOException
     */
    public static Parameters importParameters(File file,
        Class<? extends Parameters> parametersClass) throws IOException {
        log.info("Importing " + parametersClass.getSimpleName() + " from: " + file.getName());

        Properties propsFile = new Properties();
        try (Reader reader = new InputStreamReader(new FileInputStream(file),
            FileUtil.ZIGGY_CHARSET)) {
            propsFile.load(reader);
        }

        Map<String, String> propsMap = new HashMap<>();
        Set<Object> keys = propsFile.keySet();
        for (Object key : keys) {
            Object value = propsFile.get(key);
            propsMap.put((String) key, (String) value);
        }

        BeanWrapper<Parameters> paramsBean = new BeanWrapper<>(parametersClass);
        paramsBean.setProperties(propsMap);

        return paramsBean.getInstance();
    }

    public static void exportParameters(File file, Parameters parametersBean) throws IOException {
        BeanWrapper<Parameters> paramsBean = new BeanWrapper<>(parametersBean);

        Set<TypedParameter> properties = paramsBean.getTypedProperties();
        Properties propsFile = new Properties();

        for (TypedParameter property : properties) {
            String propName = property.getName();
            String propValue = property.getString();
            if (propValue == null) {
                // Hashtable (used by Properties) doesn't allow null values
                propValue = "";
            }
            propsFile.put(propName, propValue);
        }

        log.info(
            "Exporting " + parametersBean.getClass().getSimpleName() + " to: " + file.getName());

        try (Writer propsFileWriter = new OutputStreamWriter(new FileOutputStream(file),
            FileUtil.ZIGGY_CHARSET)) {
            propsFile.store(propsFileWriter, "exported by ParametersUtils");
        }
    }
}
