#include "ziggy_exceptions.h"

using namespace std;

void ZiggyException::exceptionMessage(ZiggyException exception, const char* file, const char* functionName,
		int line, const char* exceptionName, const char* msg) {

	cerr << file << "(" << line << "): exception in function " << functionName << endl;
	cerr << "Exception: " << exceptionName << endl;
	cerr << "What: " << exception.what() << endl;
	if (msg != NULL) {
		cerr << "Message: " << msg << endl;
	}

	//	now that the output is complete, we can throw the actual error
	throw exception;
}


