package gov.nasa.ziggy.parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
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
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Parameters importParameters(File file,
        Class<? extends Parameters> parametersClass) {
        log.info("Importing " + parametersClass.getSimpleName() + " from: " + file.getName());

        Properties propsFile = new Properties();
        try (Reader reader = new InputStreamReader(new FileInputStream(file),
            FileUtil.ZIGGY_CHARSET)) {
            propsFile.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read from file " + file.toString(), e);
        }

        Set<Object> keys = propsFile.keySet();
        Set<TypedParameter> typedParameters = new HashSet<>();
        for (Object key : keys) {
            Object value = propsFile.get(key);
            typedParameters.add(new TypedParameter((String) key, (String) value));
        }
        Parameters parametersInstance;
        try {
            parametersInstance = parametersClass.getDeclaredConstructor().newInstance();
            parametersInstance.populate(typedParameters);
            return parametersInstance;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException(e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void exportParameters(File file, Parameters parameters) {

        Set<TypedParameter> typedParameters = parameters.getParameters();
        Properties properties = new Properties();

        for (TypedParameter parameter : typedParameters) {
            String name = parameter.getName();
            String value = parameter.getString();
            if (value == null) {
                // Hashtable (used by Properties) doesn't allow null values
                value = "";
            }
            properties.put(name, value);
        }

        log.info("Exporting " + parameters.getClass().getSimpleName() + " to: " + file.getName());

        try (Writer propsFileWriter = new OutputStreamWriter(new FileOutputStream(file),
            FileUtil.ZIGGY_CHARSET)) {
            properties.store(propsFileWriter, "exported by ParametersUtils");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to file " + file.toString(), e);
        }
    }
}
