#ifndef _ziggy_signal_handler_h_
#define _ziggy_signal_handler_h_ 1

#include "ziggy_exceptions.h"
#include <cstdint>

/**
 *  This is the signal handler that gets registered using signal().  It's exposed here in case.
 * you don't want to use register_print_stack_on_signal() and want to register it yourself.
 * This function will attempt to cause the process to exit() upon completion.
 */
extern "C" void print_stack_trace_signal_handler(int32_t signalNumber);

/**
 * Attempts to print a stack trace to standard error when one of the following signals is raised:
 * SIGABRT, SIGSEGV, SIGBUS, SIGILL, SIGFPE.
 */
extern "C" void register_print_stack_on_signal();

#endif //_ziggy_signal_handler_h_

