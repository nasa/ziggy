package gov.nasa.ziggy.util;

import javassist.bytecode.ClassFile;

/**
 * This interface is called by {@link ClasspathScanner}
 *
 * @author Todd Klaus
 */
public interface ClasspathScannerListener {
    void processClass(ClassFile classFile);
}
