package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.gradle.api.GradleException;
import org.gradle.internal.os.OperatingSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo;
import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Unit test class for ZiggyCppPojo class. 
 * @author PT
 *
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
		
		//	create a temporary directory for everything
		tempDir = Files.createTempDirectory("ZiggyCpp").toFile();
		tempDir.deleteOnExit();
		
		//	directory for includes
		new File(tempDir, "include").mkdir();
		
		//	directory for source
		srcDir = new File(tempDir, "src");
		srcDir.mkdir();
		
		//	build directory
		buildDir = new File(tempDir,"build");
		
		//	directory for libraries
		new File(buildDir,"lib").mkdir();
		
		//	directory for includes
		new File(buildDir, "include").mkdir();
		
		//	directory for built source
		new File(buildDir, "src").mkdir();
		
		//	directory for objects
		new File(buildDir, "obj").mkdir();
		
		//	directory for executables
		new File(buildDir, "bin").mkdir();
		
		//	create C++ source and header files
		createSourceFiles();
		
		//	create the ZiggyCpp object
		ziggyCppObject = createZiggyCppObject(buildDir);
		
	}
	
	@After
	public void after() throws IOException {
		
		//	explicitly delete the temp directory
		FileUtils.deleteDirectory(tempDir);
		
		//	delete any cppdebug system properties
		System.clearProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME);
		
		//	delete the ZiggyCpp object
		ziggyCppObject = null;
		buildDir = null;
		tempDir = null;
	}
		
//***************************************************************************************
	
	//	here begins the test methods
	
	/**
	 * Tests all getter and setter methods.  
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
		assertEquals(tempDir.getAbsolutePath() + "/src", ziggyCppObject.getCppFilePaths().get(0));
		
		testStringListSettersAndGetters("IncludeFilePaths", new String[] {
				tempDir.getAbsolutePath() + "/src", 
				tempDir.getAbsolutePath() + "/include"});
		testStringListSettersAndGetters("CompileOptions", new String[] {
				"Wall", "fPic"});
		testStringListSettersAndGetters("ReleaseOptimizations", new String[] {
				"O2", "DNDEBUG", "g"});
		testStringListSettersAndGetters("DebugOptimizations", new String[] {
				"Og", "g"});
		ziggyCppObject.setLibraries(dummyArguments);
		testStringListSettersAndGetters("Libraries", new String[]{"DuMMY", "dUmMy"});
		ziggyCppObject.setLibraryPaths(dummyArguments);
		testStringListSettersAndGetters("LibraryPaths", new String[]{"DuMMY", "dUmMy"});
		ziggyCppObject.setLinkOptions(dummyArguments);
		testStringListSettersAndGetters("LinkOptions", new String[]{"DuMMY", "dUmMy"});
		
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
	 * Tests the ability to find C/C++ source files in the source directory and add them to
	 * the ZiggyCppPojo as a list of File objects
	 */
	@Test
	public void testGetCppFiles() {
		List<File> cppFiles = ziggyCppObject.getCppFiles();
		assertEquals(2, cppFiles.size());
		List<String> cppFilePaths = cppFiles.stream().map(s -> s.getAbsolutePath())
				.collect(Collectors.toList());
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp"));
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/GetString.cpp"));
		
		cppFiles = ziggyCppObject.getCppFiles();
		assertEquals(2, cppFiles.size());
		cppFilePaths = cppFiles.stream().map(s -> s.getAbsolutePath())
				.collect(Collectors.toList());
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp"));
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/GetString.cpp"));
	}
	
	@Test
	public void testGetCppFilesMultipleDirectories() throws FileNotFoundException {
		
		//	put a source directory in build, and populate it
		new File(buildDir, "src/cpp").mkdirs();
		createAdditionalSource();
		
		//	create the list of directories to check out
		List<Object> cppPaths = new ArrayList<>();
		cppPaths.add(tempDir.toString() + "/src");
		cppPaths.add(buildDir.toString() + "/src/cpp");
		ziggyCppObject.setCppFilePaths(cppPaths);
		List<File> cppFiles = ziggyCppObject.getCppFiles();
		int nFiles = cppFiles.size();
		assertEquals(3, nFiles);
		
		List<String> cppFilePaths = cppFiles.stream().map(s -> s.getAbsolutePath())
				.collect(Collectors.toList());
		assertTrue(cppFilePaths.contains(buildDir.getAbsolutePath() + "/src/cpp/GetAnotherString.cpp"));
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp"));
		assertTrue(cppFilePaths.contains(tempDir.getAbsolutePath() + "/src/GetString.cpp"));
	}
	
	/**
	 * Tests the argListToString method, which converts a list of arguments to a string, with a common
	 * prefix added to each list element
	 */
	@Test
	public void argListToStringTest() {
		String compileOptionString = ziggyCppObject.argListToString(ziggyCppObject.getCompileOptions(), 
				"-");
		assertEquals("-Wall -fPic ", compileOptionString);
	}
	
	/**
	 * Tests the code that determines the File that is to be the output of the compile and link
	 * process. 
	 */
	@Test
	public void populateBuiltFileTest() {
		
		//	executable
		ziggyCppObject.setOutputType("executable");
		File builtFile = ziggyCppObject.getBuiltFile();
		assertEquals(buildDir.getAbsolutePath() + "/bin/dummy", builtFile.getAbsolutePath());
		
		//	shared library
		ZiggyCppPojo ziggyCppShared = createZiggyCppObject(buildDir);
		ziggyCppShared.setOutputType("shared");
		ziggyCppShared.setOperatingSystem(OperatingSystem.MAC_OS);
		builtFile = ziggyCppShared.getBuiltFile();
		String builtFilePath = builtFile.getAbsolutePath();
		String sharedObjectFileType = ".dylib";
		assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType, builtFilePath);
		ziggyCppShared = createZiggyCppObject(buildDir);
		ziggyCppShared.setOutputType("shared");
		ziggyCppShared.setOperatingSystem(OperatingSystem.LINUX);
		builtFile = ziggyCppShared.getBuiltFile();
		builtFilePath = builtFile.getAbsolutePath();
		sharedObjectFileType = ".so";
		assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType, builtFilePath);
		
		//	static library
		ZiggyCppPojo ziggyCppStatic = createZiggyCppObject(buildDir);
		ziggyCppStatic.setOutputType("static");
		builtFile = ziggyCppStatic.getBuiltFile();
		builtFilePath = builtFile.getAbsolutePath();
		assertEquals(buildDir.getAbsolutePath() + "/lib/libdummy.a", builtFilePath);
	}
	
	/**
	 * Tests the process of converting a source File to an object file name (with the 
	 * path of the former stripped away).
	 */
	@Test
	public void objectNameFromSourceFileTest() {
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
	public void generateCompileCommandTest() {
		File f1 = new File("/tmp/dummy/s1.c");
		String compileCommand = ziggyCppObject.generateCompileCommand(f1);
		String expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o "
				+ "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath() + 
				"/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.c";
		assertEquals(expectedString, compileCommand);
		
		//	set up for debugging
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
		compileCommand = ziggyCppObject.generateCompileCommand(f1);
		expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o "
				+ "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath() + 
				"/include -Wall -fPic -Og -g /tmp/dummy/s1.c";
		assertEquals(expectedString, compileCommand);
		
		//	have the debugging property but set to false
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
		compileCommand = ziggyCppObject.generateCompileCommand(f1);
		expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o "
				+ "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath() + 
				"/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.c";
		assertEquals(expectedString, compileCommand);	
		
		//	test .cpp file type
		f1 = new File("/tmp/dummy/s1.cpp");
		compileCommand = ziggyCppObject.generateCompileCommand(f1);
		expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/s1.o "
				+ "-I" + tempDir.getAbsolutePath() + "/src -I" + tempDir.getAbsolutePath() + 
				"/include -Wall -fPic -O2 -DNDEBUG -g /tmp/dummy/s1.cpp";
		assertEquals(expectedString, compileCommand);	
	}
	
	/**
	 * Tests the method that generates link commands. 
	 */
	@Test
	public void generateLinkCommandTest() {
		
		configureLinkerOptions(ziggyCppObject);
		ziggyCppObject.setOutputType("executable");
		String linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/bin/dummy -L/dummy1/lib -L/dummy2/lib "
				+ "-u whatevs -O2 -DNDEBUG -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);
		
		//	now try it with debug enabled
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/bin/dummy -L/dummy1/lib -L/dummy2/lib "
				+ "-u whatevs -Og -g o1.o o2.o -lhdf5 -lnetcdf ", linkString);		
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
		
		
		//	Now for a shared object library
		ziggyCppObject = createZiggyCppObject(buildDir);
		configureLinkerOptions(ziggyCppObject);
		ziggyCppObject.setOutputType("shared");
		ziggyCppObject.setOperatingSystem(OperatingSystem.LINUX);
		String sharedObjectFileType = ".so";
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType
				+ " -L/dummy1/lib -L/dummy2/lib -shared"
				+ " o1.o o2.o -lhdf5 -lnetcdf ", linkString);
		
		//	For a Mac, there has to be an install name as well
		ziggyCppObject = createZiggyCppObject(buildDir);
		configureLinkerOptions(ziggyCppObject);
		ziggyCppObject.setOutputType("shared");
		ziggyCppObject.setOperatingSystem(OperatingSystem.MAC_OS);
		sharedObjectFileType = ".dylib";
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType
				+ " -L/dummy1/lib -L/dummy2/lib -shared"
				+ " -install_name /dev/null/rootDir/build/lib/libdummy.dylib "
				+ "o1.o o2.o -lhdf5 -lnetcdf ", linkString);
		
		//	debug enabled shouldn't do anything
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/libdummy" + sharedObjectFileType
				+ " -L/dummy1/lib -L/dummy2/lib -shared"
				+ " -install_name /dev/null/rootDir/build/lib/libdummy.dylib "
				+ "o1.o o2.o -lhdf5 -lnetcdf ", linkString);
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
		
		//	static library case
		ziggyCppObject = createZiggyCppObject(buildDir);
		configureLinkerOptions(ziggyCppObject);
		ziggyCppObject.setOutputType("static");
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("ar rs " + buildDir.getAbsolutePath() + "/lib/libdummy.a o1.o o2.o ", linkString);
		
		//	debug enabled shouldn't do anything
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "true");
		linkString = ziggyCppObject.generateLinkCommand();
		assertEquals("ar rs " + buildDir.getAbsolutePath() + "/lib/libdummy.a o1.o o2.o ", linkString);
		System.setProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME, "false");
		
	}
	
	/**
	 * Tests the method that executes the main action (compiles and links).
	 * @throws ExecuteException
	 * @throws IOException
	 */
	@Test
	public void actionTest() throws ExecuteException, IOException {
		
		//	set values for the ZiggyCppPojo 
		ziggyCppObject.setOutputName("testOutput");
		ziggyCppObject.setOutputType("executable");
		
		//	set the mocked executor into the object
		ziggyCppObject.setDefaultExecutor(defaultExecutor);
		InOrder executorCalls = Mockito.inOrder(defaultExecutor);
		
		//	call the method
		ziggyCppObject.action();
		
		//	check the calls to the executor and their order
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "GetString.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "ZiggyCppMain.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, "obj"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateLinkCommand()));
		
		//	test that the include files were copied
		File buildInclude = new File(buildDir, "include");
		File buildInclude1 = new File(buildInclude, "ZiggyCppMain.h");
		assertTrue(buildInclude1.exists());
		File buildInclude2 = new File(buildInclude, "ZiggyCppLib.h");
		assertTrue(buildInclude2.exists());

		
		//	create a new object for linking a shared object
		ziggyCppObject = createZiggyCppObject(buildDir);
		ziggyCppObject.setOutputName("testOutput");
		ziggyCppObject.setOutputType("shared");
		ziggyCppObject.setDefaultExecutor(defaultExecutor);
		ziggyCppObject.action();
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "GetString.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "ZiggyCppMain.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, "obj"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateLinkCommand()));
		
		//	and once more for a static library
		//	create a new object for linking a shared object
		ziggyCppObject = createZiggyCppObject(buildDir);
		ziggyCppObject.setOutputName("testOutput");
		ziggyCppObject.setOutputType("static");
		ziggyCppObject.setDefaultExecutor(defaultExecutor);
		ziggyCppObject.action();
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "GetString.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(tempDir, "src"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateCompileCommand(new File(srcDir, "ZiggyCppMain.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, "obj"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppObject.new CommandLineComparable(
				ziggyCppObject.generateLinkCommand()));
		
	}
	
	//	The following tests exercise various error conditions that return GradleExceptions. There are 2 error
	//	conditions that are not tested by these methods, they occur when the DefaultExecutor throws an IOException. 
	//	These cases are considered sufficiently trivial that we omit them. 
	
	/**
	 * Tests that a null value for the C++ file path produces the correct error. 
	 */
	@Test
	public void testCppFilePathNullError() {
		ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
//		exception.expect(GradleException.class);
//		exception.expectMessage("C++ file path is null");
		assertThrows("C++ file path is null", GradleException.class, () -> {
		    ziggyCppError.getCppFiles();		
		});
	}
	
	/**
	 * Tests that a nonexistent C++ file path produces the correct warning message. 
	 */
	@Test
	public void testCppFilePathDoesNotExist() {
		ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
		ziggyCppError.setCppFilePath("/this/path/does/not/exist");
		ziggyCppError.getCppFiles();
		List<String> w = ziggyCppError.loggerWarnings();
		assertTrue(w.size() > 0);
		assertTrue(w.contains("C++ file path /this/path/does/not/exist does not exist"));
	}
	
	/**
	 * Tests that a missing output name produces the correct error.
	 */
	@Test
	public void testErrorNoOutputName() {
		ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
		ziggyCppError.setCppFilePath(tempDir.getAbsolutePath() + "/src");
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
		ziggyCppError.setCppFilePath(tempDir.getAbsolutePath() + "/src");
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
		List<String> cppPaths = new ArrayList<>();
		ziggyCppError.setCppFilePath(tempDir.getAbsolutePath() + "/src");
		ziggyCppError.setBuildDir(buildDir);
		assertThrows("Both output name and output type must be specified", GradleException.class, 
		    () -> {
		        ziggyCppError.getBuiltFile();
		    });
	}
	
	/**
	 * Tests that a non-zero compiler return value produces the correct error. 
	 * @throws ExecuteException
	 * @throws IOException
	 */
	@Test
	public void testCompilerError() throws ExecuteException, IOException {
		ziggyCppObject.setOutputType("executable");
		ziggyCppObject.setDefaultExecutor(defaultExecutor);
		when(defaultExecutor.execute(any(CommandLine.class))).thenReturn(1);
		assertThrows("Compilation of file GetString.cpp failed", GradleException.class, 
		    () -> {
		        ziggyCppObject.action();
		    });
	}
	
	/**
	 * Tests that a non-zero linker return value produces the correct error. 
	 * @throws ExecuteException
	 * @throws IOException
	 */
	@Test
	public void testLinkerError() throws ExecuteException, IOException {
		ziggyCppObject.setOutputType("executable");
		ziggyCppObject.setDefaultExecutor(defaultExecutor);
		String linkerCommand = ziggyCppObject.generateLinkCommand() + "GetString.o ZiggyCppMain.o ";
		when(defaultExecutor.execute(ziggyCppObject.new CommandLineComparable(linkerCommand))).thenReturn(1);
		assertThrows("Link / library construction of dummy failed", GradleException.class, 
		    () -> {
		        ziggyCppObject.action();
		    });
	}
	
	/**
	 * Test that an invalid OS produces the correct error. 
	 */
	@Test
	public void testInvalidOsError() {
		ZiggyCppPojo ziggyCppError = new ZiggyCppPojo();
		ziggyCppError.setBuildDir(buildDir);
		ziggyCppError.setOperatingSystem(OperatingSystem.WINDOWS);
		ziggyCppError.setOutputName("dummy");
		ziggyCppError.setOutputType("shared");
		assertThrows("ZiggyCpp class does not support OS " + ziggyCppError.getOperatingSystem().getName(),
		    GradleException.class, () -> {
		        ziggyCppError.getBuiltFile();
		    });
	}

//***************************************************************************************
	
	//	here begins assorted setup and helper methods
	
	/**
	 * Creates four source files for use in the test. Two are C++ files in the tempDir/src
	 * directory, ZiggyCppMain.cpp and GetString.cpp. One is the header file ZiggyCppMain.h,
	 * also in tempDir/src. The final one is ZiggyCppLib.h, in tempdir/include.
	 * @throws FileNotFoundException
	 */
	public void createSourceFiles() throws FileNotFoundException {
		
		// content for ZiggyCppMain.cpp
		String[] mainSourceContent = 
			{"#include \"ZiggyCppMain.h\"" ,
			 "#include <iostream>",
			 "#include \"ZiggyCppLib.h",
			 "",
			 "using namespace std;",
			 "",
			 "int main(int argc, const char* argv[]) {", 
			 "    string s = getString();",
			 "    cout << s << endl;",
			 "}"
			};
		
		//	content for the getString function
		String[] getStringContent = 
			{"#include \"ZiggyCppMain.h\"", 
			 "",
			 "using namespace std;",
			 "",
			 "string getString() {",
			 "    return string(\"hello world!\");",
			 "}"
			};
		
		//	content for the ZiggyCppMain.h header
		String[] ziggyCppMainHeaderContent = 
			{"#ifndef ZIGGY_CPP",
			 "#define ZIGGY_CPP",
			 "#endif",
			 "",
			 "string getString();"
			};
		
		//	content for the ZiggyCppLib.h header
		String[] ziggyCppLibHeaderContent = 
			{"#ifndef ZIGGY_CPP_LIB",
			 "#define ZIGGY_CPP_LIB",
			 "#endif"
			};
		
		//	create the source files first
		PrintWriter mainSource = new PrintWriter(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.cpp");
		for (String line : mainSourceContent) {
			mainSource.println(line);
		}
		mainSource.close();
		
		PrintWriter getStringSource = new PrintWriter(tempDir.getAbsolutePath() + "/src/GetString.cpp");
		for (String line : getStringContent) {
			getStringSource.println(line);
		}
		getStringSource.close();
		
		//	put the main header in the src directory
		PrintWriter h1 = new PrintWriter(tempDir.getAbsolutePath() + "/src/ZiggyCppMain.h");
		for (String line : ziggyCppMainHeaderContent) {
			h1.println(line);
		}
		h1.close();
		
		//	put the other header in the include directory
		PrintWriter h2 = new PrintWriter(tempDir.getAbsolutePath() + "/include/ZiggyCppLib.h");
		for (String line : ziggyCppLibHeaderContent) {
			h2.println(line);
		}
		h2.close();
		
	}
	
	/**
	 * Add an additional source file in another directory to test accumulating files from multiple directories
	 * @throws FileNotFoundException
	 */
	public void createAdditionalSource() throws FileNotFoundException {

		//	content for the getString function
		String[] getStringContent = 
			{"#include \"ZiggyCppMain.h\"", 
			 "",
			 "using namespace std;",
			 "",
			 "string getAnotherString() {",
			 "    return string(\"hello again world!\");",
			 "}"
			};

		PrintWriter getStringSource = new PrintWriter(buildDir.getAbsolutePath() + "/src/cpp/GetAnotherString.cpp");
		for (String line : getStringContent) {
			getStringSource.println(line);
		}
		getStringSource.close();

	}
	
	/**
	 * Creates a ZiggyCpp object that is properly formed and has the following members populated:
	 * cppFilePath
	 * includeFilePaths
	 * compileOptions
	 * name
	 * Project (with a mocked Project object)
	 * @return properly-formed ZiggyCpp object
	 */ 
	@SuppressWarnings("serial")
	public ZiggyCppPojo createZiggyCppObject(File buildDir) {
		ZiggyCppPojo ziggyCppObject = new ZiggyCppPojo();
		ziggyCppObject.setCppFilePath(tempDir.getAbsolutePath() + "/src");
		ziggyCppObject.setIncludeFilePaths(new ArrayList<String>(){{
			add(tempDir.getAbsolutePath() + "/src");
			add(tempDir.getAbsolutePath() + "/include");
		}});
		ziggyCppObject.setCompileOptions(new ArrayList<String>() {{
			add("Wall");
			add("fPic");
		}});
		ziggyCppObject.setReleaseOptimizations(new ArrayList<String>() {{
			add("O2");
			add("DNDEBUG");
			add("g");
		}});
		ziggyCppObject.setDebugOptimizations(new ArrayList<String>() {{
			add("Og");
			add("g");
		}});
		ziggyCppObject.setOutputName("dummy");
		ziggyCppObject.setBuildDir(buildDir);
		ziggyCppObject.setCppCompiler("/dev/null/g++");
		ziggyCppObject.setRootDir(rootDir);
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
		for (int i=0 ; i<nOrigValues ; i++) {
			assertEquals(initialValues[i], initialGetValues.get(i));
		}
		
		List<String> replacementValues = new ArrayList<>();
		replacementValues.add("R1");
		replacementValues.add("R2");
		setMethod.invoke(ziggyCppObject, replacementValues);
		List<?> replacementGetValues = List.class.cast(getMethod.invoke(ziggyCppObject));
		assertEquals(replacementValues.size(), replacementGetValues.size());
		for (int i=0 ; i<replacementValues.size(); i++) {
			assertEquals(replacementValues.get(i), replacementGetValues.get(i));
		}
		
 	}
	
	/**
	 * Set up linker options. This needs to be done for several ZiggyCppPojo instances so we can test
	 * all of the different link types (shared, static, executable).
	 * @param ziggyCppObject object in need of link options.
	 */
	public void configureLinkerOptions(ZiggyCppPojo ziggyCppObject) {
		//	first we need to add some object files
		File o1 = new File(buildDir, "obj/o1.o");
		File o2 = new File(buildDir, "obj/o2.o");
		ziggyCppObject.setObjectFiles(o1);
		ziggyCppObject.setObjectFiles(o2);
		
		//	also some linker options and libraries
		List<String> linkerOptions = new ArrayList<>();
		linkerOptions.add("u whatevs");
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
