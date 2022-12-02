/*
 * Hdf5Interface.h
 *
 *  Created on: May 17, 2019
 *      Author: PT
 */

#ifndef HDF5INTERFACE_H_
#define HDF5INTERFACE_H_

#include "hdf5.h"
#include <vector>
#include <cstdint>
#include <string>
#include <Persistable.h>
#include <iostream>
#include <map>
#include <algorithm>

class Hdf5Interface {

public:
	Hdf5Interface(hid_t parentHdf5Id, std::string name);
	~Hdf5Interface();

	template<class T> T readScalar(hid_t hdf5Type)  {
		T contents;
		if (datasetId != -1) {
			H5Dread(datasetId, hdf5Type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &contents);
		} else {
			contents = (T)0;
		}
		return contents;
	}

	template<class T> void writeScalar(T scalarValue) {
		H5Dwrite(datasetId, datatypeId, dataspaceId, H5S_ALL, H5P_DEFAULT, &scalarValue);
	}

	std::string readString();
	void writeString(std::string s);

	template <class T> T* readArray(hid_t hdf5Type) {

		if (datasetId == -1) {
			return NULL;
		}

		//	determine the size of the return array
		int nDims = this->getNDims();
		hsize_t* dims = this->getDims();
		hsize_t numel = 1;
		for (int iDim=0 ; iDim<nDims ; iDim++) {
			numel *= dims[iDim];
		}

		//	allocate the array
		T* returnArray = new T[numel];

		//	read the array from HDF5
		H5Dread(datasetId, hdf5Type, H5S_ALL, H5S_ALL, H5P_DEFAULT, returnArray);

		return returnArray;

	}


	std::vector<std::string> readStringArray();


	template <class T> void writeArray(hid_t hdf5Type, T* content) {

		if (datasetId == -1) {
			return;
		}
		H5Dwrite(datasetId, hdf5Type, H5S_ALL, H5S_ALL, H5P_DEFAULT, content);

	}

	void writeStringArray(std::vector<std::string> s);

	hsize_t* getDims() const;
	int getNDims() const;
	long* getPersistableArrayDims() const;

	hid_t get_hdf5GroupId() const {return this->hdf5GroupId ; }
	void writeStringVector(hsize_t* offset, std::vector<std::string> content);
	static std::vector<bool> int8ToBoolVector(std::vector<int8_t> int8Vector);
	static std::vector<int8_t> boolToInt8Vector(std::vector<bool> boolVector);
	static void openAndReadHdf5Group(Persistable& obj, hid_t hdf5ParentGroupId, std::string groupName);
	static void createAndWriteHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
			std::string groupName, int32_t fieldOrder);
	static void createAndWriteHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
			std::string groupName, bool isEmpty, int32_t fieldOrder, bool parallelFlag);
	static void createAndWriteParallelHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
			std::string groupName, bool isEmpty, int32_t fieldOrder);
	static Hdf5Interface* Hdf5InterfaceForWriting(hid_t parentHdf5Id, std::string name,
			hid_t datatypeId, hsize_t nDims, hsize_t* dims, int32_t typeInt,
			bool booleanArray, int32_t fieldOrder);

private:
	hid_t parentHdf5Id;
	hid_t hdf5GroupId;
	std::string name;
	hid_t datasetId;
	hid_t dataspaceId;
	hid_t datatypeId; // used for writing only, ignored for reading
	bool datatypeNeedsToBeClosed;

	constexpr static char* EMPTY_FIELD_ATT_NAME = (char*)"EMPTY_FIELD";
	constexpr static char* OBJECT_ARRAY_DIMS_ATT_NAME = (char*)"STRUCT_OBJECT_ARRAY_DIMS";
	constexpr static char* FIELD_DATA_TYPE_ATT_NAME = (char*)"DATA_TYPE";
	constexpr static char* OBJECT_ARRAY_ATT_NAME = (char*)"STRUCT_OBJECT_ARRAY";
	constexpr static char* STRING_ARRAY_ATT_NAME = (char*)"STRING_ARRAY";
	constexpr static char* BOOLEAN_ARRAY_ATT_NAME = (char*)"LOGICAL_BOOLEAN_ARRAY";
	constexpr static char* FIELD_ORDER_ATT_NAME = (char*)"FIELD_ORDER";
	constexpr static char* PARALLEL_ARRAY_ATT_NAME = (char *)"PARALLEL_ARRAY";
	const static int32_t HDF5_PERSISTABLE_TYPE_INT = 9;

	const static int MIN_COMPRESSION_ELEMENTS = 200;
	const static uint COMPRESSION_LEVEL = 0;
	const static int MAX_BYTES_PER_HYPERSLAB = 2000000000;

	static std::map<hid_t, int> typeSizes;

	Hdf5Interface(hid_t parentHdf5Id_p, std::string name_p, hid_t datatypeId_p);
	void addMarkerAttribute(char* attributeName);
	void addScalarInt32Attribute(char* attributeName, int32_t attributeValue);
	hsize_t* chunkSize(hsize_t nDims, hsize_t* dims);

};

#endif /* HDF5INTERFACE_H_ */
