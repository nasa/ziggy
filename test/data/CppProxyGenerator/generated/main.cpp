#include <iostream>

#include "TestInputs.h"
#include "TestOutputs.h"

#include "TestController.h"

#include <BinaryInputStream.h>
#include <BinaryOutputStream.h>
#include <IOHandler.h>

int main(int argc, char **argv){

	if( argc != 3 ){
		std::cerr << "Usage: " << argv[0] << " directory-name id" << std::endl;
		exit(-1);
	}

	std::string dir = (const char*) argv[1];
	std::string id = (const char*) argv[2];

	IOHandler ioHandler( dir, id );

	TestInputs input;
	TestOutputs output;

	ioHandler.loadInput( input );

	TestController testController;
	testController.doScience( input, output );

	ioHandler.saveOutput( output );
}

