package gov.nasa.ziggy.uow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.util.RegexBackslashManager;

/**
 * Defines a UOW generator in which the units of work are based on the subdirectories of a parent
 * directory. The class uses an instance of {@link TaskConfigurationParameters} to specify a regular
 * expression that is used to identify the units of work. This allows the user to specify that some
 * but not all subdirectories are to be included, or that the subdirectories should be further than
 * 1 level below the parent directory.
 *
 * @author PT
 */
public abstract class DirectoryUnitOfWorkGenerator implements UnitOfWorkGenerator {

    private static final Logger log = LoggerFactory.getLogger(DirectoryUnitOfWorkGenerator.class);

    public static final String DIRECTORY_PROPERTY_NAME = "directory";
    public static final String REGEX_PROPERTY_NAME = "taskDirectoryRegex";

    @Override
    public List<Class<? extends Parameters>> requiredParameterClasses() {
        List<Class<? extends Parameters>> requiredParameterClasses = new ArrayList<>();
        requiredParameterClasses.add(TaskConfigurationParameters.class);
        return requiredParameterClasses;
    }

    /**
     * Convenience method that extracts the directory from a UOW instance constructed by a subclass
     * of {@link DirectoryUnitOfWorkGenerator}.
     *
     * @param uow
     * @return
     */
    public static String directory(UnitOfWork uow) {
        String clazz = uow.getParameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME)
            .getString();
        try {
            Class<?> cls = Class.forName(clazz);
            if (DirectoryUnitOfWorkGenerator.class.isAssignableFrom(cls)) {
                return uow.getParameter(DIRECTORY_PROPERTY_NAME).getString();
            } else {
                throw new PipelineException(
                    "Class " + clazz + " not a subclass of DirectoryUnitOfWorkGenerator");
            }
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Generator class " + clazz + " not found", e);
        }
    }

    /**
     * Returns the directory to be used as the top-level directory for generation of UOW instances,
     * as a {@link Path}.
     */
    protected abstract Path rootDirectory();

    @Override
    public List<UnitOfWork> generateTasks(Map<Class<? extends Parameters>, Parameters> parameters) {
        String taskDirectoryRegex = taskDirectoryRegex(parameters);
        List<UnitOfWork> unitsOfWork = new ArrayList<>();

        // If there's no taskDirectoryRegex, then return a single task with no directory field.
        // This will signal to the pipeline module that the parent directory itself should be
        // used for the unit of work.
        if (taskDirectoryRegex == null || taskDirectoryRegex.isEmpty()) {
            UnitOfWork uow = new UnitOfWork();
            uow.addParameter(
                new TypedParameter(DIRECTORY_PROPERTY_NAME, "", ZiggyDataType.ZIGGY_STRING));
            uow.addParameter(
                new TypedParameter(REGEX_PROPERTY_NAME, "", ZiggyDataType.ZIGGY_STRING));
            unitsOfWork.add(uow);
            return unitsOfWork;
        }

        // build a Pattern from the task dir regex
        Pattern taskDirPattern = Pattern.compile(taskDirectoryRegex);

        // determine the number of directory levels below the datastore root
        int dirLevelsCount = taskDirectoryRegex.split("/").length;

        // Get all directories below datastore root down to the specified depth
        log.info("Searching for UOW directories in parent directory " + rootDirectory().toString());
        try (Stream<Path> allDirs = Files.walk(rootDirectory(), dirLevelsCount)) {
            List<Path> taskDirs = allDirs.filter(Files::isDirectory)
                .map(s -> rootDirectory().relativize(s))
                .filter(s -> taskDirPattern.matcher(s.toString()).matches())
                .collect(Collectors.toList());
            log.info("Located " + taskDirs.size() + " subdirectories to parent directory");
            for (Path taskDir : taskDirs) {
                log.info("Processing directory " + taskDir.toString());
                UnitOfWork uow = new UnitOfWork();
                uow.addParameter(new TypedParameter(DIRECTORY_PROPERTY_NAME, taskDir.toString(),
                    ZiggyDataType.ZIGGY_STRING));
                uow.addParameter(new TypedParameter(REGEX_PROPERTY_NAME, taskDirectoryRegex,
                    ZiggyDataType.ZIGGY_STRING));

                unitsOfWork.add(uow);
            }
        } catch (IOException e) {
            throw new PipelineException("IO Exception occurred when attempting to construct UOWs",
                e);
        }
        return unitsOfWork;
    }

    /**
     * Returns the regular expression that defines the directory names that are allowed to become
     * units of work. Broken out as a separate method to allow overriding.
     */
    protected String taskDirectoryRegex(Map<Class<? extends Parameters>, Parameters> parameters) {
        TaskConfigurationParameters taskConfigurationParameters = (TaskConfigurationParameters) parameters
            .get(TaskConfigurationParameters.class);
        if (taskConfigurationParameters == null) {
            return new String();
        }
        String bareRegex = taskConfigurationParameters.getTaskDirectoryRegex();
        if (bareRegex == null || bareRegex.isEmpty()) {
            return new String();
        }
        return RegexBackslashManager.toSingleBackslash(bareRegex);
    }

    @Override
    public String briefState(UnitOfWork uow) {

        String directory = uow.getParameter(DIRECTORY_PROPERTY_NAME).getString();
        String taskDirectoryRegex = uow.getParameter(REGEX_PROPERTY_NAME).getString();
        if (directory.isEmpty()) {
            return rootDirectory().toString();
        }
        if (taskDirectoryRegex.isEmpty()) {
            return directory;
        }
        // If the regex has captured groups, they become the brief state of
        // the UOW. Otherwise, the full dir is the brief state
        Matcher matcher = Pattern.compile(taskDirectoryRegex).matcher(directory);
        matcher.matches();
        StringBuilder briefStateBuilder = new StringBuilder();
        if (matcher.groupCount() > 0) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                briefStateBuilder.append(matcher.group(i));
                if (i < matcher.groupCount()) {
                    briefStateBuilder.append(",");
                }
            }
        } else {
            briefStateBuilder.append(matcher.group(0));
        }
        return briefStateBuilder.toString();

    }

}
