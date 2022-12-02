#include "ziggy_exceptions.h"

using namespace std;

void simpleFunction(uint32_t i) {
    if (i < 10) {
        RUN_TIME_STACK_TRACE
        stringstream errMsg;
        errMsg << "i must be greater than or equals to 10, but was " << i;
        ZIGGY_THROW(IllegalArgumentException, errMsg.str());
    }
    try {
        simpleFunction(i - 1);
    } catch (ZiggyException& e) {  //Notice this is not a const reference.
        ZIGGY_RETHROW(e);
    }
}

int main(int argc, char** argv) {
    
    try {
        RUN_TIME_STACK_TRACE
        ZIGGY_THROW(IllegalArgumentException, "Err msg.");
    } catch (const ZiggyException& e) {
        cerr << e << endl;
    }
    
    
    try {
        simpleFunction(15);
    } catch (const ZiggyException& e) {
        cerr << e << endl;
    } 
        
    return 0;
}

