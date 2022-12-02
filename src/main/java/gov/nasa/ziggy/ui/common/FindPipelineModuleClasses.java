package gov.nasa.ziggy.ui.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineModule;

/**
 * @author Todd Klaus
 */
public class FindPipelineModuleClasses {
    public FindPipelineModuleClasses() {
    }

    public void report() throws Exception {
        ClasspathUtils classpathUtils = new ClasspathUtils();
        Set<Class<? extends PipelineModule>> detectedClasses = classpathUtils
            .scanFully(PipelineModule.class);
        Map<String, List<String>> packageMap = new HashMap<>();

        for (Class<? extends PipelineModule> clazz : detectedClasses) {
            String className = clazz.getSimpleName();
            String packageName = clazz.getPackage().getName();

            List<String> classesForPackage = packageMap.get(packageName);

            if (classesForPackage == null) {
                classesForPackage = new ArrayList<>();
                packageMap.put(packageName, classesForPackage);
            }
            classesForPackage.add(className);
        }

        List<String> packageNames = new LinkedList<>(packageMap.keySet());
        Collections.sort(packageNames);

        for (String packageName : packageNames) {
            System.out.println(packageName);

            List<String> classNames = packageMap.get(packageName);

            for (String className : classNames) {
                System.out.println("  " + className);
            }
        }

        System.out.println("Done, found " + detectedClasses.size() + " classes");
    }

    public static void main(String[] args) throws Exception {
        FindPipelineModuleClasses finder = new FindPipelineModuleClasses();
        finder.report();
    }
}
