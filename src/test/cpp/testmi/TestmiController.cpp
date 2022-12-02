
#include "TestmiController.h"

#include <iostream>

TestmiController::TestmiController(){
}

TestmiController::~TestmiController(){
}

int TestmiController::doScience( TestmiInputs *inputs, TestmiOutputs* outputs ){

	try{

		// populate inputs

		// invoke algorithm, populate outputs

		// store outputs

	}catch (...){
		std::cerr << "Unexpected error thrown" << std::endl;
		return -3;
	}

	return 0;
}

