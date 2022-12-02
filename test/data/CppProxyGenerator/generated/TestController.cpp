
#include "TestController.h"

//#include "libtest.h"

#include <iostream>

TestController::TestController(){
}

TestController::~TestController(){
}

int TestController::doScience( TestInputs& inputs, TestOutputs& outputs ){

	outputs.floatArray1 = inputs.floatArray1;
	outputs.floatArray2 = inputs.floatArray2;
	outputs.string1 = inputs.string1;
	outputs.pixelData1 = inputs.pixelData1;
	outputs.pixelData2 = inputs.pixelData2;
	outputs.pixelData3 = inputs.pixelData3;
	outputs.pixelData4 = inputs.pixelData4;
	outputs.stringList1 = inputs.stringList1;
	outputs.stringMap1 = inputs.stringMap1;
	outputs.stringSet1 = inputs.stringSet1;
	outputs.pixelMap1 = inputs.pixelMap1;
	
	outputs.char1 = inputs.char1;
	outputs.byte1 = inputs.byte1;
	outputs.short1 = inputs.short1;
	outputs.long1 = inputs.long1;
	outputs.float1 = inputs.float1;
	outputs.double1 = inputs.double1;
	outputs.boolean1 = inputs.boolean1;
	outputs.int1 = inputs.int1;

	std::cout << "sizeof(char) = " << sizeof(char) << std::endl;
	std::cout << "sizeof(short) = " << sizeof(short) << std::endl;
	std::cout << "sizeof(long) = " << sizeof(long) << std::endl;
	std::cout << "sizeof(float) = " << sizeof(float) << std::endl;
	std::cout << "sizeof(double) = " << sizeof(double) << std::endl;
	std::cout << "sizeof(bool) = " << sizeof(bool) << std::endl;
	std::cout << "sizeof(int) = " << sizeof(int) << std::endl;
	
	// Call application and library initialization. Perform this 
	// initialization before calling any API functions or
	// Compiler-generated libraries.
//	if (!mclInitializeApplication(NULL,0)){
//		std::cerr << "could not initialize the application properly"
//			<< std::endl;
//		return -1;
//	}

//	if( !libtestInitialize() ){
//		std::cerr << "could not initialize the library properly"
//			<< std::endl;
//		return -1;
//	}

//	try{

		// populate inputs

		// create outputs

		// invoke MATLAB function

		// store outputs

//	}catch (const mwException& e){
//		std::cerr << e.what() << std::endl;
//		return -2;
//	}catch (...){
//		std::cerr << "Unexpected error thrown" << std::endl;
//		return -3;
//	}

	// Call the application and library termination routine
//	libtestTerminate();

//	mclTerminateApplication();
//	return 0;
}

