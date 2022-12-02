#ifndef TESTMICONTROLLER_H_
#define TESTMICONTROLLER_H_

#include "TestmiInputs.h"
#include "TestmiOutputs.h"

class TestmiController
{
	public:
	TestmiController();
	virtual ~TestmiController();

	int doScience( TestmiInputs *inputs, TestmiOutputs *outputs );
};

#endif /*TESTMICONTROLLER_H_*/

