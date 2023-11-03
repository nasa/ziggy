
#ifndef _ziggy_exceptions_h_
#define _ziggy_exceptions_h_ 1

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <errno.h>
#include <algorithm>
#include <cstdint>
#include <exception>

//These includes are for the backtrace functionality
#include <string.h>
#include <execinfo.h>
#include <stdlib.h>


#define MAX_STACK_DEPTH 128
#define RUN_TIME_STACK_TRACE \
{ \
    void* stackPointers[MAX_STACK_DEPTH]; \
    uint32_t traceCount = backtrace (stackPointers, MAX_STACK_DEPTH); \
    char** symbols = backtrace_symbols(stackPointers, traceCount); \
    if (symbols != nullptr) { \
        for (uint32_t frameIndex=0; frameIndex < traceCount;  frameIndex++) { \
            std::cerr << symbols[frameIndex] << std::endl; \
        } \
        free(symbols); \
    } \
}

/**
 * Concrete base class for exceptions.
 */
class ZiggyException : public virtual std::exception {
public:
    ZiggyException(const char* whatMessage_p) : whatMessage_m(whatMessage_p) {}
    ZiggyException(const std::string& whatMessage_p) : whatMessage_m(whatMessage_p) {}
    ZiggyException(const ZiggyException& src) = default;
    
    virtual const char* what() const noexcept {
        return whatMessage_m.c_str();
    }
    
    
    virtual ~ZiggyException() {
    }

private:
    std::string whatMessage_m;
};

template<typename T>
void exceptionMessage(T exception, const char* file, const char* functionName,
		int line, const char* exceptionName, const char* msg) {

	std::cerr << file << "(" << line << "): exception in function " << functionName << std::endl;
	std::cerr << "Exception: " << exceptionName << std::endl;
	std::cerr << "What: " << exception.what() << std::endl;
	if (msg != NULL) {
		std::cerr << "Message: " << msg << std::endl;
	}

	//	now that the output is complete, we can throw the actual error
	throw exception;
}

/**
 * Base class for I/O errors.
 */
class IoException : public ZiggyException {
public:
    IoException(const char* whatMessage_p) : ZiggyException(whatMessage_p) {}
    IoException(const std::string& whatMessage_p) : ZiggyException(whatMessage_p) {}
    IoException(const IoException& src) = default;  
    
    virtual ~IoException() {
    }
};

#define ZIGGY_THROW(ExceptionTypeName) \
{ \
	exceptionMessage(ExceptionTypeName(#ExceptionTypeName), __FILE__, __FUNCTION__, __LINE__, \
		#ExceptionTypeName, NULL); \
\}


#define ZIGGY_THROW_MSG(ExceptionTypeName, Msg) \
{ \
	exceptionMessage(ExceptionTypeName(#ExceptionTypeName), __FILE__, __FUNCTION__, __LINE__, \
		#ExceptionTypeName, Msg); \
}

#endif

