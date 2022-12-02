#ifndef TESTCONTROLLER_H_
#define TESTCONTROLLER_H_

#include "TestInputs.h"
#include "TestOutputs.h"

class TestController
{
	public:
	TestController();
	virtual ~TestController();

	int doScience( TestInputs& inputs, TestOutputs& outputs );
};

#endif /*TESTCONTROLLER_H_*/

