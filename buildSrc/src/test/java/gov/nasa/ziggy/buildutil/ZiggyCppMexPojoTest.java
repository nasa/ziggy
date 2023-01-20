package gov.nasa.ziggy.buildutil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.gradle.api.GradleException;
import org.gradle.internal.os.OperatingSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

public class ZiggyCppMexPojoTest {

	File tempDir = null;
	File buildDir = null;
	File rootDir = null;
	File projectDir = null;
	File srcDir = null;
	File incDir = null;
	ZiggyCppMexPojo ziggyCppMexObject = null;
	
	DefaultExecutor defaultExecutor = Mockito.mock(DefaultExecutor.class);
	
	@Before
	public void before() throws IOException {
		
		//	create a temporary directory for everything
		tempDir = Files.createTempDirectory("rootDir").toFile();
		tempDir.deleteOnExit();
		
		//	rootDir is the same as tempDir
		rootDir = tempDir;
		
		//	projectDir
		projectDir = new File(rootDir,"projectDir");
		projectDir.mkdir();
		
		//	build directory under project
		buildDir = new File(projectDir, "build");
		buildDir.mkdir();
		
		//	lib, bin, obj, and include directories under build
		new File(buildDir, "lib").mkdir();
		new File(buildDir, "obj").mkdir();
		new File(buildDir, "bin").mkdir();
		new File(buildDir, "include").mkdir();
		
		//	add a source directory that's several levels down
		srcDir = new File(projectDir, "src/main/cpp/mex");
		srcDir.mkdirs();
		
		//	add an include directory that's several levels down
		incDir = new File(projectDir, "src/main/include");
		incDir.mkdirs();
		
		//	create source files
		createSourceFiles();
		
		//	create the ZiggyCppMexPojo object
		ziggyCppMexObject = createZiggyCppMexPojo();
	}
	
	@After
	public void after() throws IOException {
		
		//	explicitly delete the temp directory
		FileUtils.deleteDirectory(tempDir);
		
		//	delete any cppdebug system properties
		System.clearProperty(ZiggyCppPojo.CPP_DEBUG_PROPERTY_NAME);
		
		//	delete the ZiggyCpp object
		ziggyCppMexObject = null;
		buildDir = null;
		tempDir = null;
		projectDir = null;
		srcDir = null;
		incDir = null;
	}
	
//***************************************************************************************
	
	//	Here begins the actual test classes
	
	/** Tests that the output type setters have no effect on the output type
	 * 
	 */
	@Test
	public void testOutputTypeSetters() {
		ziggyCppMexObject.setOutputType("executable");
		assertEquals(ziggyCppMexObject.getOutputType(), BuildType.SHARED);
		ziggyCppMexObject.setOutputType("static");
		assertEquals(ziggyCppMexObject.getOutputType(), BuildType.SHARED);
		ziggyCppMexObject.setOutputType(BuildType.EXECUTABLE);
		assertEquals(ziggyCppMexObject.getOutputType(), BuildType.SHARED);
		ziggyCppMexObject.setOutputType(BuildType.STATIC);
		assertEquals(ziggyCppMexObject.getOutputType(), BuildType.SHARED);
	}
	
	/**
	 * Tests the setters and getters that are unique to the ZiggyCppMexPojo (the ones
	 * that are inherited from ZiggyCppPojo are not tested).
	 */
	@Test
	public void testSettersAndGetters() {
		
		//	these getter tests implicitly test the setters in createZiggyCppMexPojo():
		assertEquals(projectDir.getAbsolutePath(), ziggyCppMexObject.getProjectDir().getAbsolutePath());
		assertEquals("/dev/null/MATLAB_R2017b", ziggyCppMexObject.getMatlabPath());
		List<String> mexfileNames = ziggyCppMexObject.getMexfileNames();
		assertEquals(2, mexfileNames.size());
		assertEquals("CSource1", mexfileNames.get(0));
		assertEquals("CppSource2", mexfileNames.get(1));
	}
	
	/**
	 * Tests the compile command generator, in particular to make certain that the MATLAB include
	 * path and MATLAB_MEX_FILE compiler directive are present
	 */
	@Test
	public void testGenerateCompileCommand() {
		
		//	test with debug options disabled
		String compileString = ziggyCppMexObject.generateCompileCommand(new File("/dev/null/dmy1.c"));
		String expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/dmy1.o "
				+ "-I" + srcDir.getAbsolutePath() + " -I" + incDir.getAbsolutePath()
				+ " -I/dev/null/MATLAB_R2017b/extern/include -Wall -fPic -DMATLAB_MEX_FILE -O2 -DNDEBUG -g "
				+ "/dev/null/dmy1.c";
		assertEquals(expectedString, compileString);
		
		//	test with debug options enabled
		System.setProperty("cppdebug", "true");
		compileString = ziggyCppMexObject.generateCompileCommand(new File("/dev/null/dmy1.c"));
		expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/dmy1.o "
				+ "-I" + srcDir.getAbsolutePath() + " -I" + incDir.getAbsolutePath()
				+ " -I/dev/null/MATLAB_R2017b/extern/include -Wall -fPic -DMATLAB_MEX_FILE -Og -g "
				+ "/dev/null/dmy1.c";
		assertEquals(expectedString, compileString);
		
		//	test with debug property present but set to false
		System.setProperty("cppdebug", "false");
		compileString = ziggyCppMexObject.generateCompileCommand(new File("/dev/null/dmy1.c"));
		expectedString = "/dev/null/g++ -c -o " + buildDir.getAbsolutePath() + "/obj/dmy1.o "
				+ "-I" + srcDir.getAbsolutePath() + " -I" + incDir.getAbsolutePath()
				+ " -I/dev/null/MATLAB_R2017b/extern/include -Wall -fPic -DMATLAB_MEX_FILE -O2 -DNDEBUG -g "
				+ "/dev/null/dmy1.c";
		assertEquals(expectedString, compileString);
	}
	
	@Test
	public void testGenerateSharedObjectName() {
		String generatedName = ziggyCppMexObject.generateSharedObjectName();
		assertEquals("projectDir-src-main-cpp-mex", generatedName);
	}
	
	@Test
	public void testGenerateLinkCommand() {
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		String linkCommand = ziggyCppMexObject.generateLinkCommand();
		String expectedCommand = "/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/"
				+ "libdummy.so -L/dummy1/lib -L/dummy2/lib "
				+"-L/dev/null/MATLAB_R2017b/bin/glnxa64 -shared o1.o o2.o -lhdf5 -lnetcdf -lmex -lmx -lmat ";
		assertEquals(expectedCommand, linkCommand);

		//	now test the library name for empty object name
		ziggyCppMexObject = createZiggyCppMexPojo();
		ziggyCppMexObject.setOutputName("");
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		linkCommand = ziggyCppMexObject.generateLinkCommand();
		expectedCommand = "/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/"
				+ "libprojectDir-src-main-cpp-mex.so -L/dummy1/lib -L/dummy2/lib "
				+"-L/dev/null/MATLAB_R2017b/bin/glnxa64 -shared o1.o o2.o -lhdf5 -lnetcdf -lmex -lmx -lmat ";
		assertEquals(expectedCommand, linkCommand);

		//	test for Mac OS
		ziggyCppMexObject = createZiggyCppMexPojo();
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.MAC_OS);
		linkCommand = ziggyCppMexObject.generateLinkCommand();
		expectedCommand = "/dev/null/g++ -o " + buildDir.getAbsolutePath() + "/lib/"
				+ "libdummy.dylib -L/dummy1/lib -L/dummy2/lib "
				+"-L/dev/null/MATLAB_R2017b/bin/maci64 "
				+"-shared -install_name " + rootDir.getAbsolutePath()+"/build/lib/libdummy.dylib"
				+ " o1.o o2.o -lhdf5 -lnetcdf -lmex -lmx -lmat ";
		assertEquals(expectedCommand, linkCommand);
	}
	
	@Test
	public void testGenerateMexCommand() {
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		configureLinkerOptions(ziggyCppMexObject);
		File mexfile = new File(buildDir, "lib/o1.mexmaci64");
		File objFile = new File(buildDir, "obj/o1.o");
		String mexCommand = ziggyCppMexObject.generateMexCommand(mexfile, objFile);
		String expectedCommand = "/dev/null/g++ -o " + mexfile.getAbsolutePath() + " "
				+ objFile.getAbsolutePath() + " -L/dummy1/lib -L/dummy2/lib "
				+ "-L/dev/null/MATLAB_R2017b/bin/glnxa64 -L" + buildDir.getAbsolutePath() + "/lib "
				+ "-lhdf5 -lnetcdf -lmex -lmx -lmat -ldummy -shared";
		assertEquals(expectedCommand, mexCommand);
		
		//	test for empty library object name
		ziggyCppMexObject = createZiggyCppMexPojo();
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOutputName("");
		mexCommand = ziggyCppMexObject.generateMexCommand(mexfile, objFile);	
		expectedCommand = "/dev/null/g++ -o " + mexfile.getAbsolutePath() + " "
				+ objFile.getAbsolutePath() + " -L/dummy1/lib -L/dummy2/lib "
				+ "-L/dev/null/MATLAB_R2017b/bin/glnxa64 -L" + buildDir.getAbsolutePath() + "/lib "
				+ "-lhdf5 -lnetcdf -lmex -lmx -lmat -lprojectDir-src-main-cpp-mex -shared";
		assertEquals(expectedCommand, mexCommand);	
		
		//	test for Mac OS
		ziggyCppMexObject = createZiggyCppMexPojo();
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.MAC_OS);
		mexCommand = ziggyCppMexObject.generateMexCommand(mexfile, objFile);	
		expectedCommand = "/dev/null/g++ -o " + mexfile.getAbsolutePath() + " "
				+ objFile.getAbsolutePath() + " -L/dummy1/lib -L/dummy2/lib "
				+ "-L/dev/null/MATLAB_R2017b/bin/maci64 -L" + buildDir.getAbsolutePath() + "/lib "
				+ "-lhdf5 -lnetcdf -lmex -lmx -lmat -ldummy -shared";
		assertEquals(expectedCommand, mexCommand);			
	}
	
	@Test
	public void testAction() throws ExecuteException, IOException {
		
		//	set the mocked executor into the object
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		ziggyCppMexObject.setDefaultExecutor(defaultExecutor);
		InOrder executorCalls = Mockito.inOrder(defaultExecutor);
		
		//	call the method
		ziggyCppMexObject.action();

		//	check the calls -- first the 4 compile commands
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(projectDir, 
				"src/main/cpp/mex"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateCompileCommand(new File(srcDir, "CSource1.c"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(projectDir, 
				"src/main/cpp/mex"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateCompileCommand(new File(srcDir, "CSource2.c"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(projectDir, 
				"src/main/cpp/mex"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateCompileCommand(new File(srcDir, "CppSource1.cpp"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(projectDir, 
				"src/main/cpp/mex"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateCompileCommand(new File(srcDir, "CppSource2.cpp"))));
		
		//	then the link command for the dynamic library (and also make sure that 2 of the 4 files
		//	got removed from the list of object files)
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, 
				"obj"));
		List<File> allObjectFiles = ziggyCppMexObject.getObjectFiles();
		assertEquals(2, allObjectFiles.size());
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateLinkCommand()));
		
		//	then the mex commands
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, 
				"obj"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateMexCommand(new File(buildDir, "lib/CSource1.mexa64"),
						new File(buildDir, "obj/CSource1.o"))));
		executorCalls.verify(defaultExecutor).setWorkingDirectory(new File(buildDir, 
				"obj"));
		executorCalls.verify(defaultExecutor).execute(ziggyCppMexObject.new CommandLineComparable(
				ziggyCppMexObject.generateMexCommand(new File(buildDir, "lib/CppSource2.mexa64"),
						new File(buildDir, "obj/CppSource2.o"))));		
		
	}
	
	//	Here are unit tests that exercise various error cases
	
	@SuppressWarnings("serial")
	@Test
	public void testErrorMexfileMissingSourceFile() {
		ziggyCppMexObject.setMexfileNames(new ArrayList<String>() {{
			add("CSource3");
		}});
		ziggyCppMexObject.setDefaultExecutor(defaultExecutor);
		assertThrows("No object file for mexfile CSource3", GradleException.class, () -> {
		    ziggyCppMexObject.action();
		});
	}
	
	@Test
	public void testErrorMexReturnCode() throws ExecuteException, IOException {
		ziggyCppMexObject.setDefaultExecutor(defaultExecutor);
		configureLinkerOptions(ziggyCppMexObject);
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.LINUX);
		File mexfile = new File(buildDir, "lib/CSource1.mexa64");
		File objFile = new File(buildDir, "obj/CSource1.o");
		String mexCommand = ziggyCppMexObject.generateMexCommand(mexfile, objFile);
		when(defaultExecutor.execute(ziggyCppMexObject.new CommandLineComparable(
				mexCommand))).thenReturn(1);
		assertThrows("Mexing of file CSource1.mexa64 failed", GradleException.class, () -> {
		    ziggyCppMexObject.action();
		});
	}
	
	@Test
	public void testBadMexSuffix() {
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.WINDOWS);
		assertThrows(GradleException.class, () -> {
		    ziggyCppMexObject.mexSuffix();
		});
	}
	
	@Test
	public void testBadMatlabArch() {
		ziggyCppMexObject.setOperatingSystem(OperatingSystem.WINDOWS);
		assertThrows(GradleException.class, () -> {
		    ziggyCppMexObject.matlabArch();
		});
	}
	
	@SuppressWarnings("serial")
	@Test
	public void testNoBuildDir() {
		ZiggyCppMexPojo ziggyCppMexObject = new ZiggyCppMexPojo();
		ziggyCppMexObject.setMexfileNames(new ArrayList<String>() {{
			add("CSource1");
			add("CppSource2");
		}});
		assertThrows("buildDir and mexfileNames must not be null", GradleException.class, 
		    () -> {
		        ziggyCppMexObject.populateMexfiles();
		    });
	}
	
	@Test
	public void testNoMexfiles() {
		ZiggyCppMexPojo ziggyCppMexObject = new ZiggyCppMexPojo();
		ziggyCppMexObject.setBuildDir(buildDir);
		assertThrows("buildDir and mexfileNames must not be null", GradleException.class, 
		    () -> {
		        ziggyCppMexObject.populateMexfiles();
		    });
	}
	
//***************************************************************************************
	
	//	here begins assorted setup and helper methods
	
	public void createSourceFiles() throws IOException {
		
		//	create 4 temporary "C/C++" files in the source directory
		new File(srcDir, "CSource1.c").createNewFile();
		new File(srcDir, "CSource2.c").createNewFile();
		new File(srcDir, "CppSource1.cpp").createNewFile();
		new File(srcDir, "CppSource2.cpp").createNewFile();
		
		new File(srcDir, "Header1.h").createNewFile();
		new File(incDir, "Header2.hpp").createNewFile();
	}
	
	@SuppressWarnings("serial")
	public ZiggyCppMexPojo createZiggyCppMexPojo() {
		ZiggyCppMexPojo ziggyCppMexObject = new ZiggyCppMexPojo();
		ziggyCppMexObject.setBuildDir(buildDir);
		ziggyCppMexObject.setProjectDir(projectDir);
		ziggyCppMexObject.setRootDir(rootDir);
		ziggyCppMexObject.setCppCompiler("/dev/null/g++");
		ziggyCppMexObject.setCppFilePath(srcDir.getAbsolutePath());
		ziggyCppMexObject.setMatlabPath("/dev/null/MATLAB_R2017b");
		ziggyCppMexObject.setOutputName("dummy");
		ziggyCppMexObject.setMexfileNames(new ArrayList<String>() {{
			add("CSource1");
			add("CppSource2");
		}});
		ziggyCppMexObject.setIncludeFilePaths(new ArrayList<String>() {{
			add(srcDir.getAbsolutePath());
			add(incDir.getAbsolutePath());
		}});
		ziggyCppMexObject.setCompileOptions(new ArrayList<String>() {{
			add("Wall");
			add("fPic");
		}});
		ziggyCppMexObject.setReleaseOptimizations(new ArrayList<String>() {{
			add("O2");
			add("DNDEBUG");
			add("g");
		}});
		ziggyCppMexObject.setDebugOptimizations(new ArrayList<String>() {{
			add("Og");
			add("g");
		}});
		return ziggyCppMexObject;
	}

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
