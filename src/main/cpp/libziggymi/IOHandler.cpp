
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <cstdint>
#include <sys/stat.h>

#include <time.h>

#include "IOHandler.h"
#include "hdf5.h"

IOHandler::IOHandler(const std::string& dir_p, const std::string& id_p, const std::string& binaryName_p) : 
    dir(dir_p), id(id_p), binaryName(binaryName_p) {
    
    if (binaryName.rfind("/") != std::string::npos) {
        binaryName = binaryName.substr(binaryName.rfind("/") + 1);
        inputFilename = dir + "/" + binaryName + "-inputs-" + id + ".h5";
        outputFilename = dir + "/" + binaryName + "-outputs-" + id + ".h5";
    }
}
    

void IOHandler::loadInputs(Persistable& inputs) const {

    time_t startTime;
	char* inputFilenameCharArray = new char[inputFilename.length()+1];
	strcpy(inputFilenameCharArray, inputFilename.c_str());
	hid_t fileId = H5Fopen(inputFilenameCharArray, H5F_ACC_RDWR, H5P_DEFAULT);
	delete[] inputFilenameCharArray;
	startTime = time(NULL);
	inputs.readHdf5(fileId);
	H5Fclose(fileId);

    time_t elapsedTime = time(NULL) - startTime;
    std::cerr << "load time = " << elapsedTime << " secs" << std::endl;
}

bool IOHandler::fileExists(const std::string& name) {
	std::ifstream f(name.c_str());
	return f.good();
}


void IOHandler::saveOutputs(Persistable& outputs) const {

    time_t startTime;
	char* outputFilenameCharArray = new char[outputFilename.length()+1];
	strcpy(outputFilenameCharArray, outputFilename.c_str());
	H5Pset_libver_bounds(H5P_FILE_ACCESS_DEFAULT, H5F_LIBVER_V18, H5F_LIBVER_V18);
	hid_t fileId = H5Fcreate(outputFilenameCharArray, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	delete[] outputFilenameCharArray;
	startTime = time(NULL);
	outputs.writeHdf5(fileId);
	H5Fclose(fileId);
    
    time_t elapsedTime = time(NULL) - startTime;
    std::cerr << "save time = " << elapsedTime << " secs" << std::endl;
}

