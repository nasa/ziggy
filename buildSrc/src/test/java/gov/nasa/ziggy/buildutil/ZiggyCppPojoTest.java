package gov.nasa.ziggy.buildutil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.gradle.api.GradleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Unit test class for ZiggyCppPojo class.
 *
 * @author PT
 */
public class ZiggyCppPojoTest {

    File tempDir = null;
    File buildDir = null;
    File srcDir = null;
    File rootDir = new File("/dev/null/rootDir");
    ZiggyCppPojo ziggyCppObject;

    DefaultExecutor defaultExecutor = Mockito.mock(DefaultExecutor.class);

    @Before
    public void before() throws IOException {

        // create a temporary directory for everything
        // create a temporary directory for everything
        tempDir = Files
            .createDirectories(Paths.get("build").resolve("test").resolve("ZiggyCppMexPojoTest"))
            .toFile()
            .getAbsoluteFile();

        // directory for includes
        new File(tempDir, "include").mkdir();

        // directory for source
        srcDir = new File(tempDir, "src");
        srcDir.mkdir();

        // build directory
        buildDir = new File(tempDir, "build");

        // directory for libraries
        new File(buildDir, "lib").mkdir();

        // directory for includes
        new File(buildDir, "include").mkdir();

        // directory for built source
        new File(buildDir, "src").mkdir();

        // directory for objects
        new File(buildDir, "obj").mkdir();

        // directory for executables
        new File(buildDir, "bin").mkdir();

        // create C++ source and header files
        createSourceFiles();

        // create the ZiggyCpp object
        ziggyCppObject = createZiggyCppObject(buildDir);
    }

    @After
    public void after() throws IOException {

        // explicitly delete the temp directory
        FileUtils.deleteDirectory(tempDir);

        // delete any cppdebug system properties
        System.clearProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME);

        // delete the ZiggyCpp object
        ziggyCppObject = null;
        buildDir = null;
        tempDir = null;
    }

//***************************************************************************************

    // here begins the test methods

    /**
     * Tests all getter and setter methods.
     *
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    @Test
    public void getterSetterTest() throws NoSuchMethodException, IllegalAccessException,
        IllegalArgumentException, InvocationTargetException {

        List<String> dummyArguments = new ArrayList<>();
        dummyArguments.add("DuMMY");
        dummyArguments.add("dUmMy");
        dummyArguments.add("");
        assertEquals(tempDir.getAbsolutePath() + "/src",
            ziggyCppObject.getSourceFilePaths().get(0));

        testStringListSettersAndGetters("IncludeFilePaths", new String[] {
            tempDir.getAbsolutePath() + "/src", tempDir.getAbsolutePath() + "/include" });
        testStringListSettersAndGetters("cppCompileOptions", new String[] { "-Wall", "-fPic" });
        testStringListSettersAndGetters("cCompileOptions", new String[] { "-fPic" });
        testStringListSettersAndGetters("ReleaseOptimizations",
            new String[] { "-O2", "-DNDEBUG", "-g" });
        testStringListSettersAndGetters("DebugOptimizations", new String[] { "-Og", "-g" });
        ziggyCppObject.setLibraries(dummyArguments);
        testStringListSettersAndGetters("Libraries", new String[] { "DuMMY", "dUmMy" });
        ziggyCppObject.setLibraryPaths(dummyArguments);
        testStringListSettersAndGetters("LibraryPaths", new String[] { "DuMMY", "dUmMy" });
        ziggyCppObject.setLinkOptions(dummyArguments);
        testStringListSettersAndGetters("LinkOptions", new String[] { "DuMMY", "dUmMy" });

        ziggyCppObject.setOutputName("outputName");
        assertEquals("outputName", ziggyCppObject.getOutputName());

        ziggyCppObject.setOutputType(BuildType.EXECUTABLE);
        assertEquals(BuildType.EXECUTABLE, ziggyCppObject.getOutputType());
        ziggyCppObject.setOutputType("executable");
        assertEquals(BuildType.EXECUTABLE, ziggyCppObject.getOutputType());

        ziggyCppObject.setOutputType(BuildType.SHARED);
        assertEquals(BuildType.SHARED, ziggyCppObject.getOutputType());
        ziggyCppObject.setOutputType("shared");
        assertEquals(BuildType.SHARED, ziggyCppObject.getOutputType());

        ziggyCppObject.setOutputType(BuildType.STATIC);
        assertEquals(BuildType.STATIC, ziggyCppObject.getOutputType());
        ziggyCppObject.setOutputType("static");
        assertEquals(BuildType.STATIC, ziggyCppObject.getOutputType());

        assertEquals(buildDir, ziggyCppObject.getBuildDir());
        assertEquals(buildDir.getAbsolutePath(), ziggyCppObject.getBuildDir().getAbsolutePath());
    }

    /**
     * Tests the ability to find C/C++ source files in the source directory and add them to the
     * ZiggyCppPojo as a list of File objects
     */
    @Test
    public void testGetCppFiles() {
        List<File> cppFiles = ziggyCppObject.getSourceFiles();
        assertEquals(3, cppFiles.size());
        List<String> cppFilePaths = cppFiles.stream()
            .map(File::getAbsolutePath)
            .collect(Collectors.toList());
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp"));
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/GetString.cpp"));
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCModule.c"));

        // Now check that the compiler choices are set correctly.
        Map<File, String> sourceFilesWithCompiler = ziggyCppObject.getSourceFiles(false);
        String cppCompiler = ZiggyCppPojo.Compiler.CPP.compiler();
        String cCompiler = ZiggyCppPojo.Compiler.C.compiler();
        assertEquals(cppCompiler, sourceFilesWithCompiler
            .get(new File(tempDir.getAbsolutePath(), "/src/ZiggyCppMain.cpp")));
        assertEquals(cppCompiler,
            sourceFilesWithCompiler.get(new File(tempDir.getAbsolutePath(), "/src/GetString.cpp")));
        assertEquals(cCompiler, sourceFilesWithCompiler
            .get(new File(tempDir.getAbsolutePath(), "/src/ZiggyCModule.c")));
    }

    @Test
    public void testGetCppFilesMultipleDirectories() throws FileNotFoundException {

        // put a source directory in build, and populate it
        new File(buildDir, "src/cpp").mkdirs();
        createAdditionalSource();

        // create the list of directories to check out
        List<Object> cppPaths = new ArrayList<>();
        cppPaths.add(tempDir.toString() + "/src");
        cppPaths.add(buildDir.toString() + "/src/cpp");
        ziggyCppObject.setSourceFilePaths(cppPaths);
        List<File> cppFiles = ziggyCppObject.getSourceFiles();
        int nFiles = cppFiles.size();
        assertEquals(4, nFiles);

        List<String> cppFilePaths = cppFiles.stream()
            .map(File::getAbsolutePath)
            .collect(Collectors.toList());
        assertTrue(
            cppFilePaths.contains(buildDir.getAbsolutePath() + "/src/cpp/GetAnotherString.cpp"));
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp"));
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/GetString.cpp"));
        assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCModule.c"));
    }

    /**
     * Tests the argListToString method, which converts a list of arguments to a string, with a
     * common prefix added to each list element
     */
    @Test
    public void testArgListToString() {
        String compileOptionString = ziggyCppObject
            .argListToString(ziggyCppObject.getcppCompileOptions(), "");
        assertEquals("-Wall -fPic ", compileOptionString);
    }

    /**
     * Tests the code that determines the File that is to be the output of the compile and link
     * process.
     */
    @Test
    public void testPopulateBuiltFile() {

        // executable
        ziggyCppObject.setOutputType("executable");
        File builtFile = ziggyCppObject.getBuiltFile();
        assertEquals(buildDir.getAbsolutePath() + "/bin/dummy", builtFile.getAbsolutePath());

        // shared library
        ZiggyCppPojo ziggyCppShared = createZiggyCppObject(buildDir);
        ziggyCppShared.setOutputType("shared");
        ziggyCppShared.setArchitecture(SystemArchitecture.MAC_INTEL);
        builtFile = ziggyCppShared.getBuiltFile();
        String builtFilePath = builtFile.getAbsolutePath();
        String sharedObjectFileType = ".dylib";
        assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType,
            builtFilePath);
        ziggyCppShared = createZiggyCppObject(buildDir);
        ziggyCppShared.setOutputType("shared");
        ziggyCppShared.setArchitecture(SystemArchitecture.LINUX_INTEL);
        builtFile = ziggyCppShared.getBuiltFile();
        builtFilePath = builtFile.getAbsolutePath();
        sharedObjectFileType = ".so";
        assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType,
            builtFilePath);

        // static library
        ZiggyCppPojo ziggyCppStatic = createZiggyCppObject(buildDir);
        ziggyCppStatic.setOutputType("static");
        builtFile = ziggyCppStatic.getBuiltFile();
        builtFilePath = builtFile.getAbsolutePath();
        assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy.a", builtFilePath);

        // Now with a user-specified output directory
        ziggyCppObject.setOutputDir("foo");
        builtFile = ziggyCppObject.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/dummy", builtFile.getAbsolutePath());

        ziggyCppShared.setOutputDir("foo");
        builtFile = ziggyCppShared.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/libdummy.so",
            builtFile.getAbsolutePath());

        ziggyCppStatic.setOutputDir("foo");
        builtFile = ziggyCppStatic.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/libdummy.a",
            builtFile.getAbsolutePath());

        // Now with a user-specified output directory parent
        ziggyCppObject.setOutputDir("");
        ziggyCppObject.setOutputDirParent("foo");
        builtFile = ziggyCppObject.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/bin/dummy",
            builtFile.getAbsolutePath());

        ziggyCppShared.setOutputDir("");
        ziggyCppShared.setOutputDirParent("foo");
        builtFile = ziggyCppShared.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/lib/libdummy.so",
            builtFile.getAbsolutePath());

        ziggyCppStatic.setOutputDir("");
        ziggyCppStatic.setOutputDirParent("foo");
        builtFile = ziggyCppStatic.getBuiltFile();
        assertEquals(System.getProperty("user.dir") + "/foo/lib/libdummy.a",
            builtFile.getAbsolutePath());
    }

    /**
     * Tests the process of converting a source File to an object file name (with the path of the
     * former stripped away).
     */
    @Test
    public void testObjectNameFromSourceFile() {
        File f1 = new File("/tmp/dummy/s1.c");
        String s1 = ZiggyCppPojo.objectNameFromSourceFile(f1);
        assertEquals("s1.o", s1);
        File f2 = new File("/tmp/dummy/s1.cpp");
        String s2 = ZiggyCppPojo.objectNameFromSourceFile(f2);
        assertEquals("s1.o", s2);
    }

    /**
     * Tests the method that generates compile commands.
     */
    @Test
    public void testGenerateCompileCommand() {
        File f1 = new File("/tmp/dummy/s1.c");

        // This is a stupid way to get my map entry, but it's the way that works with Java,
        // so here we are.
        String compiler = ZiggyCppPojo.Compiler.CPP.compiler();
        String compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        String expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o "
            + "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.c";
        assertEquals(expectedString, compileCommand);

        // set up for debugging
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
        compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o " + "-I"
            + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -Og -g /tmp/dummy/s1.c";
        assertEquals(expectedString, compileCommand);

        // have the debugging property but set to false
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
        compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o " + "-I"
            + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.c";
        assertEquals(expectedString, compileCommand);

        // test .cpp file type
        f1 = new File("/tmp/dummy/s1.cpp");
        compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o " + "-I"
            + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.cpp";
        assertEquals(expectedString, compileCommand);

        // test with output dir
        ziggyCppObject.setOutputDir("foo");
        compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        expectedString = "/dev/null/g++ -c -o " + System.getProperty("user.dir") + "/foo/s1.o "
            + "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.cpp";
        assertEquals(expectedString, compileCommand);

        // Test with output dir parent
        ziggyCppObject.setOutputDir("");
        ziggyCppObject.setOutputDirParent("foo");
        compileCommand = ziggyCppObject.generateCompileCommand(f1, compiler, null, null);
        expectedString = "/dev/null/g++ -c -o " + System.getProperty("user.dir") + "/foo/obj/s1.o "
            + "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath()
            + "/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.cpp";
        assertEquals(expectedString, compileCommand);
    }

    @Test
    public void testGenerateCompileCommandWithCompiler() {

        Map<File, String> sourceFilesWithCompiler = ziggyCppObject.getSourceFiles(false);
        ZiggyCppPojo.Compiler.CPP.compiler();
        String cCompiler = ZiggyCppPojo.Compiler.C.compiler();

        for (Map.Entry<File, String> entry : sourceFilesWithCompiler.entrySet()) {
            String compileCommand = ziggyCppObject.generateCompileCommand(entry);
            entry.getKey().toString();
            entry.getKey().getName();
            String compiler = entry.getValue();
            if (compiler.equals(cCompiler)) {
                assertTrue(compileCommand.startsWith("/dev/null/gcc -c -o"));
                assertTrue(compileCommand.contains("-fPic"));
                assertFalse(compileCommand.contains("-Wall"));
            } else {
                assertTrue(compileCommand.startsWith("/dev/null/g++ -c -o"));
                assertTrue(compileCommand.contains("-fPic"));
                assertTrue(compileCommand.contains("-Wall"));
            }
        }
    }

    /**
     * Tests the method that generates link commands.
     */
    @Test
    public void testGenerateLinkCommand() {

        configureLinkerOptions(ziggyCppObject);
        ziggyCppObject.setOutputType("executable");
        String linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath()
            + "/bin/dummy -L/dummy1/lib -L/dummy2/lib "
            + "-u whatevs -O2 -DNDEBUG -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);

        // now try it with debug enabled
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath()
            + "/bin/dummy -L/dummy1/lib -L/dummy2/lib "
            + "-u whatevs -Og -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");

        // Now with an output dir configured
        ziggyCppObject.setOutputDir("foo");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + System.getProperty("user.dir")
            + "/foo/dummy -L/dummy1/lib -L/dummy2/lib "
            + "-u whatevs -O2 -DNDEBUG -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);

        // Now with an output dir parent configured
        ziggyCppObject.setOutputDir("");
        ziggyCppObject.setOutputDirParent("foo");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + System.getProperty("user.dir")
            + "/foo/bin/dummy -L/dummy1/lib -L/dummy2/lib "
            + "-u whatevs -O2 -DNDEBUG -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);

        // Now for a shared object library
        ziggyCppObject = createZiggyCppObject(buildDir);
        configureLinkerOptions(ziggyCppObject);
        ziggyCppObject.setOutputType("shared");
        ziggyCppObject.setArchitecture(SystemArchitecture.LINUX_INTEL);
        String sharedObjectFileType = ".so";
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy"
            + sharedObjectFileType + " -L/dummy1/lib -L/dummy2/lib -shared"
            + " o1.o o2.o -lhdf5 -lnetcdf ", linkString);

        // For a Mac, there has to be an install name as well
        ziggyCppObject = createZiggyCppObject(buildDir);
        configureLinkerOptions(ziggyCppObject);
        ziggyCppObject.setOutputType("shared");
        ziggyCppObject.setArchitecture(SystemArchitecture.MAC_INTEL);
        sharedObjectFileType = ".dylib";
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy"
            + sharedObjectFileType + " -L/dummy1/lib -L/dummy2/lib -shared" + " -install_name "
            + buildDir.getAbsolutePath() + "/lib/libdummy.dylib " + "o1.o o2.o -lhdf5 -lnetcdf ",
            linkString);

        // debug enabled shouldn't do anything
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy"
            + sharedObjectFileType + " -L/dummy1/lib -L/dummy2/lib -shared" + " -install_name "
            + buildDir.getAbsolutePath() + "/lib/libdummy.dylib " + "o1.o o2.o -lhdf5 -lnetcdf ",
            linkString);
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");

        // Output dir case
        ziggyCppObject.setOutputDir("foo");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("/dev/null/g++ -o " + System.getProperty("user.dir") + "/foo/libdummy"
            + sharedObjectFileType + " -L/dummy1/lib -L/dummy2/lib -shared" + " -install_name "
            + "foo/libdummy.dylib " + "o1.o o2.o -lhdf5 -lnetcdf ", linkString);

        // Output dir parent case
        ziggyCppObject.setOutputDir("");
        ziggyCppObject.setOutputDirParent("foo");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals(
            "/dev/null/g++ -o " + System.getProperty("user.dir") + "/foo/lib/libdummy"
                + sharedObjectFileType + " -L/dummy1/lib -L/dummy2/lib -shared"
                + " -install_name foo/lib/libdummy.dylib " + "o1.o o2.o -lhdf5 -lnetcdf ",
            linkString);

        // static library case
        ziggyCppObject = createZiggyCppObject(buildDir);
        configureLinkerOptions(ziggyCppObject);
        ziggyCppObject.setOutputType("static");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("ar rs " + buildDir.getAbsolutePath() + "/lib/libdummy.a o1.o o2.o ",
            linkString);

        // debug enabled shouldn't do anything
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
        linkString = ziggyCppObject.generateLinkCommand();
        assertEquals("ar rs " + buildDir.getAbsolutePath() + "/lib/libdummy.a o1.o o2.o ",
            linkString);
        System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
    }

    /**
     * Tests the method that executes the main action (compiles and links).
     *
     * @throws ExecuteException
     * @throws IOException
     */
    @Test
    public void testAction() throws ExecuteException, IOException {

        // set values for the ZiggyCppPojo
        ziggyCppObject.setOutputName("testOutput");
        ziggyCppObject.setOutputType("executable");

        // set the mocked executor into the object
        ziggyCppObject.setDefaultExecutor(defaultExecutor);
        InOrder executorCalls = Mockito.inOrder(defaultExecutor);

        String compiler = ZiggyCppPojo.Compiler.CPP.compiler();
        // call the method
        ziggyCppObject.action();

        // check the calls to the executor and their order
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject
                .generateCompileCommand(new File(srcDir, "GetString.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateCompileCommand(
                new File(srcDir, "ZiggyCppMain.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(
                ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateLinkCommand()));

        // test that the include files were copied
        File buildInclude = new File(buildDir, "include");
        File buildInclude1 = new File(buildInclude, "ZiggyCppMain.h");
        assertTrue(buildInclude1.exists());
        File buildInclude2 = new File(buildInclude, "ZiggyCppLib.h");
        assertTrue(buildInclude2.exists());

        // create a new object for linking a shared object
        ziggyCppObject = createZiggyCppObject(buildDir);
        ziggyCppObject.setOutputName("testOutput");
        ziggyCppObject.setOutputType("shared");
        ziggyCppObject.setDefaultExecutor(defaultExecutor);
        ziggyCppObject.action();
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject
                .generateCompileCommand(new File(srcDir, "GetString.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateCompileCommand(
                new File(srcDir, "ZiggyCppMain.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(
                ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateLinkCommand()));

        // and once more for a static library
        // create a new object for linking a shared object
        ziggyCppObject = createZiggyCppObject(buildDir);
        ziggyCppObject.setOutputName("testOutput");
        ziggyCppObject.setOutputType("static");
        ziggyCppObject.setDefaultExecutor(defaultExecutor);
        ziggyCppObject.action();
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject
                .generateCompileCommand(new File(srcDir, "GetString.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateCompileCommand(
                new File(srcDir, "ZiggyCppMain.cpp"), compiler, null, null)));
        executorCalls.verify(defaultExecutor)
            .execute(
                ziggyCppObject.new CommandLineComparable(ziggyCppObject.generateLinkCommand()));
    }

    // The following tests exercise various error conditions that return GradleExceptions. There are
    // 2 error
    // conditions that are not tested by these methods, they occur when the DefaultExecutor throws
    // an IOException.
    // These cases are considered sufficiently trivial that we omit them.

    /**
     * Tests that a null value for the C++ file path produces the correct error.
     */
    @Test
    public void testCppFilePathNullError() {
        ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
        assertThrows("C++ file path is null", GradleException.class, () -> {
            ziggyCppError.getSourceFiles();
        });
    }

    /**
     * Tests that a nonexistent C++ file path produces the correct warning message.
     */
    @Test
    public void testCppFilePathDoesNotExist() {
        ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
        ziggyCppError.setSourceFilePath("/this/path/does/not/exist");
        ziggyCppError.getSourceFiles(true);
        List<String> w = ziggyCppError.loggerWarnings();
        assertTrue(w.size() > 0);
        assertTrue(w.contains("Source file path /this/path/does/not/exist does not exist"));
    }

    /**
     * Tests that a missing output name produces the correct error.
     */
    @Test
    public void testErrorNoOutputName() {
        ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
        ziggyCppError.setSourceFilePath(tempDir.getAbsolutePath() + "/src");
        ziggyCppError.setBuildDir(buildDir);
        ziggyCppError.setOutputType("executable");
        assertThrows("Both output name and output type must be specified", GradleException.class,
            () -> {
                ziggyCppError.getBuiltFile();
            });
    }

    /**
     * Tests that a missing output type produces the correct error.
     */
    @Test
    public void testErrorNoOutputType() {
        ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
        ziggyCppError.setSourceFilePath(tempDir.getAbsolutePath() + "/src");
        ziggyCppError.setBuildDir(buildDir);
        ziggyCppError.setOutputName("dummy");
        assertThrows("Both output name and output type must be specified", GradleException.class,
            () -> {
                ziggyCppError.getBuiltFile();
            });
    }

    /**
     * Tests that missing both the output type and the built file name produces the correct error.
     */
    @Test
    public void testErrorNoOutputTypeOrName() {
        ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
        new ArrayList<>();
        ziggyCppError.setSourceFilePath(tempDir.getAbsolutePath() + "/src");
        ziggyCppError.setBuildDir(buildDir);
        assertThrows("Both output name and output type must be specified", GradleException.class,
            () -> {
                ziggyCppError.getBuiltFile();
            });
    }

    /**
     * Tests that a non-zero compiler return value produces the correct error.
     *
     * @throws ExecuteException
     * @throws IOException
     */
    @Test
    public void testCompilerError() throws ExecuteException, IOException {
        ziggyCppObject.setOutputType("executable");
        ziggyCppObject.setDefaultExecutor(defaultExecutor);
        when(defaultExecutor.execute(any(CommandLine.class))).thenReturn(1);
        assertThrows("Compilation of file GetString.cpp failed", GradleException.class, () -> {
            ziggyCppObject.action();
        });
    }

    /**
     * Tests that a non-zero linker return value produces the correct error.
     *
     * @throws ExecuteException
     * @throws IOException
     */
    @Ignore
    public void testLinkerError() throws ExecuteException, IOException {
        ziggyCppObject.setOutputType("executable");
        ziggyCppObject.setDefaultExecutor(defaultExecutor);
        String linkerCommand = ziggyCppObject.generateLinkCommand()
            + "GetString.o ZiggyCModule.o ZiggyCppMain.o";
        when(defaultExecutor.execute(ziggyCppObject.new CommandLineComparable(linkerCommand)))
            .thenReturn(1);
        // TODO Fix flaky test
        // There seems to be a timing issue between these two statements. On my Linux workstation
        // anyway.
        assertThrows("Link / library construction of dummy failed", GradleException.class, () -> {
            ziggyCppObject.action();
        });
    }

//***************************************************************************************

    // here begins assorted setup and helper methods

    /**
     * Creates four source files for use in the test. Two are C++ files in the tempDir/src
     * directory, ZiggyCppMain.cpp and GetString.cpp. One is the header file ZiggyCppMain.h, also in
     * tempDir/src. The final one is ZiggyCppLib.h, in tempdir/include.
     *
     * @throws FileNotFoundException
     */
    public void createSourceFiles() throws FileNotFoundException {

        // content for ZiggyCppMain.cpp
        String[] mainSourceContent = { "#include \"ZiggyCppMain.h\"", "#include <iostream>",
            "#include \"ZiggyCppLib.h", "", "using namespace std;", "",
            "int main(int argc, const char* argv[]) {", "    string s = getString();",
            "    cout << s << endl;", "}" };

        // content for the getString function
        String[] getStringContent = { "#include \"ZiggyCppMain.h\"", "", "using namespace std;", "",
            "string getString() {", "    return string(\"hello world!\");", "}" };

        // content for the ZiggyCppMain.h header
        String[] ziggyCppMainHeaderContent = { "#ifndef ZIGGY_CPP", "#define ZIGGY_CPP", "#endif",
            "", "string getString();",
            "void get_sort_index(int nLength, double *inputArray, int *sortIndex, int *iStack );" };

        // content for the ZiggyCppLib.h header
        String[] ziggyCppLibHeaderContent = { "#ifndef ZIGGY_CPP_LIB", "#define ZIGGY_CPP_LIB",
            "#endif" };

        // Content for the ZiggyCModule.c function
        String[] ziggyCModuleContent = { "#define <string.h>", "#define <stdlib.h>",
            "void get_sort_index(int nLength, double *inputArray, int *sortIndex, int *iStack ) {",
            "}" };

        // create the source files first
        PrintWriter mainSource = new PrintWriter(
            tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp");
        for (String line : mainSourceContent) {
            mainSource.println(line);
        }
        mainSource.close();

        PrintWriter getStringSource = new PrintWriter(
            tempDir.getAbsolutePath() + "/src/GetString.cpp");
        for (String line : getStringContent) {
            getStringSource.println(line);
        }
        getStringSource.close();

        PrintWriter cModuleSource = new PrintWriter(
            tempDir.getAbsolutePath() + "/src/ZiggyCModule.c");
        for (String line : ziggyCModuleContent) {
            cModuleSource.println(line);
        }
        cModuleSource.close();

        // put the main header in the src directory
        PrintWriter h1 = new PrintWriter(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.h");
        for (String line : ziggyCppMainHeaderContent) {
            h1.println(line);
        }
        h1.close();

        // put the other header in the include directory
        PrintWriter h2 = new PrintWriter(tempDir.getAbsolutePath() + "/include/ZiggyCppLib.h");
        for (String line : ziggyCppLibHeaderContent) {
            h2.println(line);
        }
        h2.close();
    }

    /**
     * Add an additional source file in another directory to test accumulating files from multiple
     * directories
     *
     * @throws FileNotFoundException
     */
    public void createAdditionalSource() throws FileNotFoundException {

        // content for the getString function
        String[] getStringContent = { "#include \"ZiggyCppMain.h\"", "", "using namespace std;", "",
            "string getAnotherString() {", "    return string(\"hello again world!\");", "}" };

        PrintWriter getStringSource = new PrintWriter(
            buildDir.getAbsolutePath() + "/src/cpp/GetAnotherString.cpp");
        for (String line : getStringContent) {
            getStringSource.println(line);
        }
        getStringSource.close();
    }

    /**
     * Creates a ZiggyCpp object that is properly formed and has the following members populated:
     * cppFilePath includeFilePaths compileOptions name Project (with a mocked Project object)
     *
     * @return properly-formed ZiggyCpp object
     */
    public ZiggyCppPojo createZiggyCppObject(File buildDir) {
        ZiggyCppPojo ziggyCppObject = new ZiggyCppPojo();
        ziggyCppObject.setSourceFilePath(tempDir.getAbsolutePath() + "/src");
        ziggyCppObject.setIncludeFilePaths(
            List.of(tempDir.getAbsolutePath() + "/src", tempDir.getAbsolutePath() + "/include"));
        ziggyCppObject.setcppCompileOptions(List.of("-Wall", "-fPic"));
        ziggyCppObject.setcCompileOptions(List.of("-fPic"));
        ziggyCppObject.setReleaseOptimizations(List.of("-O2", "-DNDEBUG", "-g"));
        ziggyCppObject.setDebugOptimizations(List.of("-Og", "-g"));
        ziggyCppObject.setOutputName("dummy");
        ziggyCppObject.setBuildDir(buildDir);
        ZiggyCppPojo.setCppCompiler("/dev/null/g++");
        ZiggyCppPojo.setCCompiler("/dev/null/gcc");
        ziggyCppObject.setRootDir(rootDir);
        ziggyCppObject.setMaxCompileThreads(1);
        return ziggyCppObject;
    }

    public void testStringListSettersAndGetters(String fieldName, String[] initialValues)
        throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException,
        InvocationTargetException {
        String getter = "get" + fieldName;
        String setter = "set" + fieldName;
        int nOrigValues = initialValues.length;
        Method getMethod = ZiggyCppPojo.class.getDeclaredMethod(getter);
        Method setMethod = ZiggyCppPojo.class.getDeclaredMethod(setter, List.class);
        List<?> initialGetValues = List.class.cast(getMethod.invoke(ziggyCppObject));
        assertEquals(nOrigValues, initialGetValues.size());
        for (int i = 0; i < nOrigValues; i++) {
            assertEquals(initialValues[i], initialGetValues.get(i));
        }

        List<String> replacementValues = new ArrayList<>();
        replacementValues.add("R1");
        replacementValues.add("R2");
        setMethod.invoke(ziggyCppObject, replacementValues);
        List<?> replacementGetValues = List.class.cast(getMethod.invoke(ziggyCppObject));
        assertEquals(replacementValues.size(), replacementGetValues.size());
        for (int i = 0; i < replacementValues.size(); i++) {
            assertEquals(replacementValues.get(i), replacementGetValues.get(i));
        }
    }

    /**
     * Set up linker options. This needs to be done for several ZiggyCppPojo instances so we can
     * test all of the different link types (shared, static, executable).
     *
     * @param ziggyCppObject object in need of link options.
     */
    public void configureLinkerOptions(ZiggyCppPojo ziggyCppObject) {
        // first we need to add some object files
        File o1 = new File(buildDir, "obj/o1.o");
        File o2 = new File(buildDir, "obj/o2.o");
        ziggyCppObject.setObjectFiles(o1);
        ziggyCppObject.setObjectFiles(o2);

        // also some linker options and libraries
        List<String> linkerOptions = new ArrayList<>();
        linkerOptions.add("-u whatevs");
        ziggyCppObject.setLinkOptions(linkerOptions);
        List<String> libraryPathOptions = new ArrayList<>();
        libraryPathOptions.add("/dummy1/lib");
        libraryPathOptions.add("/dummy2/lib");
        ziggyCppObject.setLibraryPaths(libraryPathOptions);
        List<String> libraryOptions = new ArrayList<>();
        libraryOptions.add("hdf5");
        libraryOptions.add("netcdf");
        ziggyCppObject.setLibraries(libraryOptions);
    }
}
