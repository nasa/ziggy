# Prerequisites and System Requirements

### Operating System

Ziggy is supported on Linux and macOS.

For Linux, the following versions have been tested: SLES 11, SLES 12, RHEL 7, RHEL 8, and Debian through version 11 (bullseye). That said, we expect that any relatively recent version of Linux will suffice. 

For macOS, All versions from Snow Leopard (10.6) to Monterey (12.6) have been used to build and run Ziggy. Both Intel and Apple M1 CPUs have been used. Compability of Ventura (13.x) is to be determined.

### RAM and Disk Space

Ziggy itself is quite lightweight, hence we expect that the RAM and disk space requirements will be set by the dataset you want to process and the algorithms it runs, not by Ziggy. That said: we have run Ziggy on laptops with 16 GB RAM, and the total disk space footprint for Ziggy when fully built is under 3 GB. 

### Compilers and Interpreters

Ziggy requires the following computer language resources:

- Java: JDK 1.8. Note that later versions of the JDK have some changes to their included libraries that have not yet been addressed in the Ziggy build system.
- Perl 5.16 or later.
- C/C++ compiler that supports the 2011 C++ standard. Historically we've used gcc but we expect that any compiler that's 2011-compliant will do. We've also used clang, just to prove that we could. 
- Python 3 and Pip 3. Note that these are only for the sample pipeline, not for Ziggy itself.
