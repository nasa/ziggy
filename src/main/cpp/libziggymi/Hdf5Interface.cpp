/*
 * Hdf5Interface.cpp
 *
 * Provides a data structure that contains the most important HDF5 ID values
 * (group, dataset, dataspace, datatype, name) and tools for manipulating the
 * HDF5 objects referred to by these values. Note that all these methods are
 * intended for use in auto-generated code that's only used to read and write
 * HDF5 files that are organized in a particular way, hence there is basically
 * no error-checking in the code here because we can rely on the auto-generator
 * to make sure that we only use the class and its methods under circumstances
 * where we know no error will occur.
 *  Created on: May 17, 2019
 *      Author: PT
 */

#include "Hdf5Interface.h"

/**
 * Constructor. Takes the parent group (can be a file) and the name of the group to be opened.
 * Populates the group ID, dataset ID, and dataspace ID, opening all of these. The assumption is
 * that the group contains a dataset with the same name as the group.
 */
Hdf5Interface::Hdf5Interface(hid_t parentHdf5Id_p, std::string name_p) :
	parentHdf5Id(parentHdf5Id_p), name(name_p) {

	//	open the group if it exists
	hdf5GroupId = H5Gopen2(parentHdf5Id, name.c_str(), H5P_DEFAULT);
	datatypeNeedsToBeClosed = false;

	//	capture the contents of the group
	if (!H5Aexists(hdf5GroupId, EMPTY_FIELD_ATT_NAME) &&
			!H5Aexists(hdf5GroupId, OBJECT_ARRAY_ATT_NAME)) {
		datasetId = H5Dopen(hdf5GroupId, name.c_str(), H5P_DEFAULT);
		dataspaceId = H5Dget_space(datasetId);
	} else {
		datasetId = -1 ;
		dataspaceId = -1;
	}

}

/**
 * Constructor used for cases in which HDF5 arrays are to be WRITTEN. Note that this
 * constructor is lightweight but doesn't do nearly enough of the setup that is needed
 * to make a usable Hdf5Interface object for writing, to do that use the static method
 * Hdf5InterfaceForWriting.
 */
Hdf5Interface::Hdf5Interface(hid_t parentHdf5Id_p, std::string name_p, hid_t datatypeId_p) :
		parentHdf5Id(parentHdf5Id_p), name(name_p), datatypeId(datatypeId_p) {

	//	default the group, dataspace, dataset, and datatype to -1
	hdf5GroupId = -1;
	dataspaceId = -1;
	datasetId = -1;
	datatypeNeedsToBeClosed = false;

}

/**
 * Static factory method that sets up an Hdf5Interface object for writing.
 */
Hdf5Interface* Hdf5Interface::Hdf5InterfaceForWriting(hid_t parentHdf5Id_p,
		std::string name_p, hid_t datatypeId_p, hsize_t nDims, hsize_t* dims_p, int32_t typeInt,
		bool booleanArray, int32_t fieldOrder) {

	hsize_t* dims;
	hsize_t scalarDims[1];
	//	handle the case in which the dims are null but nDims is 1
	if (nDims == 1 && dims_p == NULL) {
		dims = scalarDims;
		dims[0] = 1;
	} else {
		dims = dims_p;
	}

	//	construct the object
	Hdf5Interface* hdf5Interface = new Hdf5Interface(parentHdf5Id_p, name_p, datatypeId_p);

	//	create the HDF5 group
	hdf5Interface->hdf5GroupId = H5Gcreate(hdf5Interface->parentHdf5Id,
			hdf5Interface->name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	//	set the field order
	hdf5Interface->addScalarInt32Attribute(FIELD_ORDER_ATT_NAME, fieldOrder);

	//	if this is an empty array, set that up now
	if (nDims == 1 && dims[0] <= 0) {
		hdf5Interface->addMarkerAttribute(EMPTY_FIELD_ATT_NAME);
		return hdf5Interface;
	}

	//	populate the typeInt attribute -- this is a means of storing the data type that
	//	is independent of the HDF5 type integers, which can vary based on the version of
	//	the HDF5 library you use
	hdf5Interface->addScalarInt32Attribute(FIELD_DATA_TYPE_ATT_NAME, typeInt);

	//	if this is a scalar or array of Persistable objects, populate the appropriate
	//	attributes and return
	if (hdf5Interface->datatypeId == H5T_OPAQUE) {
		if (nDims > 1 || dims[0] > 1) { // object array case
			hdf5Interface->addMarkerAttribute(OBJECT_ARRAY_ATT_NAME);

			hid_t attributeSpace = H5Screate_simple(1, &nDims, NULL);
			hid_t attributeId = H5Acreate(hdf5Interface->hdf5GroupId, OBJECT_ARRAY_DIMS_ATT_NAME,
					H5T_NATIVE_INT64, attributeSpace, H5P_DEFAULT, H5P_DEFAULT);
			H5Awrite(attributeId, H5T_NATIVE_INT64, dims);
			H5Aclose(attributeId);
			H5Aclose(attributeSpace);
		}
		return hdf5Interface;
	}


	//	construct a property for deflation and chunking
	hid_t deflateProperty;
	long nElem = 1;
	for (uint i=0 ; i<nDims ; i++) {
		nElem *= dims[i];
	}
	if (nElem >= MIN_COMPRESSION_ELEMENTS && COMPRESSION_LEVEL > 0) {
		deflateProperty = H5Pcreate(H5P_DATASET_CREATE);
		H5Pset_chunk(deflateProperty, nDims, hdf5Interface->chunkSize(nDims, dims));
		H5Pset_deflate(deflateProperty, COMPRESSION_LEVEL);
	} else {
		deflateProperty = H5Pcopy(H5P_DEFAULT);
	}

	//	If this is a scalar or array of string objects, construct a data type for var-length
	//	strings and populate the appropriate attribute for the group
	if (hdf5Interface->datatypeId == H5T_C_S1) {
		hdf5Interface->datatypeId = H5Tcopy(hdf5Interface->datatypeId);
		hdf5Interface->datatypeNeedsToBeClosed = true;
		H5Tset_size(hdf5Interface->datatypeId, H5T_VARIABLE);
		if (nDims > 1 || dims[0] > 1) {
			hdf5Interface->addMarkerAttribute(STRING_ARRAY_ATT_NAME);
		}
	}

	//	if this was originally a boolean array that is being stored as an int8 array,
	//	because HDF5 has no native support for booleans, set the appropriate attribute now
	if (booleanArray) {
		hdf5Interface->addMarkerAttribute(BOOLEAN_ARRAY_ATT_NAME);
	}

	//	construct dataspace and dataset, including any compression desirements
	hdf5Interface->dataspaceId = H5Screate_simple(nDims, dims, NULL);
	hdf5Interface->datasetId = H5Dcreate(hdf5Interface->hdf5GroupId,
			hdf5Interface->name.c_str(), hdf5Interface->datatypeId,
			hdf5Interface->dataspaceId, H5P_DEFAULT, deflateProperty, H5P_DEFAULT);

	return hdf5Interface;
}

hsize_t* Hdf5Interface::chunkSize(hsize_t nDims, hsize_t* dims) {

	hsize_t* cSize = new hsize_t[nDims];
	for (uint i=0 ; i<nDims ; i++) {
		cSize[i] = dims[i];
	}

	int elemSize = Hdf5Interface::typeSizes[datatypeId];

	//	determine size per row
	for (uint iDim=0 ; iDim<nDims ; iDim++) {
		long nElem = 1;
		for (uint jDim=iDim+1 ; jDim<nDims ; jDim++) {
			nElem *= dims[jDim];
		}

		//	convert to bytes per "row"
		long nBytesPerRow = nElem * elemSize;

		//	determine the number of rows that can be accommodated within the desired max
		//	number of bytes
		long nRows = std::min((long)(MAX_BYTES_PER_HYPERSLAB / nBytesPerRow), (long)dims[iDim]);

		//	if nRows is zero, it means that the # of bytes per row is too large, so we have
		//	to perform the subdivision into hyperslabs at a higher dimension, and this dimension
		//	we should do 1 per hyperslab; alternately, if nRows > 0, we are done with this
		//	activity and can return
		if (nRows == 0) {
			cSize[iDim] = 1;
		} else {
			cSize[iDim] = nRows;
			break;
		}
	}
	return cSize;
}

/**
 * Destructor. Closes the dataspace, dataset, and group.
 */
Hdf5Interface::~Hdf5Interface() {
	if (dataspaceId > 0) {
		H5Sclose(dataspaceId);
	}
	if (datasetId > 0) {
		H5Dclose(datasetId);
	}
	if (datatypeNeedsToBeClosed) {
		H5Tclose(datatypeId);
	}
	if (hdf5GroupId > 0) {
		H5Gclose(hdf5GroupId);
	}
}

/**
 * Gets the dimensions of a persistable array; these are stored as an attribute of the
 * group of the top-level HDF5 group for the array.
 */
long* Hdf5Interface::getPersistableArrayDims() const {

	//	handle case of the array being empty
	htri_t hasEmptyAttribute = H5Aexists(hdf5GroupId, EMPTY_FIELD_ATT_NAME);
	if (hasEmptyAttribute > 0) {
		long* emptyDims = new long[1];
		emptyDims[0] = 0;
		return emptyDims;
	}
	hid_t attributeId = H5Aopen(hdf5GroupId, OBJECT_ARRAY_DIMS_ATT_NAME, H5P_DEFAULT);
	hid_t attributeDataspaceId = H5Aget_space(attributeId);
	int nDims = H5Sget_simple_extent_ndims(attributeDataspaceId);
	long* dims = new long[nDims];
	H5Aread(attributeId, H5T_NATIVE_INT64, dims);
	H5Sclose(attributeDataspaceId);
	H5Aclose(attributeId);
	return dims;
}

std::string Hdf5Interface::readString() {
	char * readBuffer;
	if (datasetId == -1) {
		readBuffer = (char*)"";
	} else {
		hid_t memtype = H5Tcopy(H5T_C_S1);
		H5Tset_size(memtype, H5T_VARIABLE);
		H5Dread(datasetId, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, &readBuffer);
		H5Tclose(memtype);
	}
	std::string retval = readBuffer;
	return retval;
}

void Hdf5Interface::writeString(std::string s) {
	const char* writeBuffer = s.c_str();
	H5Dwrite(datasetId, datatypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, &writeBuffer);
}

hsize_t* Hdf5Interface::getDims() const {

	//	determine the size of the dataspace
	int nDims = this->getNDims();
	hsize_t* dims = new hsize_t[nDims];
	H5Sget_simple_extent_dims(dataspaceId, dims, NULL);
	return dims;

}

int Hdf5Interface::getNDims() const {
	//	determine the size of the dataspace
	int nDims = H5Sget_simple_extent_ndims(dataspaceId);
	return nDims;
}

/**
 * Casts a boolean vector to int8. This is necessary because HDF5 does not support
 * boolean data types, so they are transported from the worker to the application
 * and back as int8's.
 */
std::vector<int8_t> Hdf5Interface::boolToInt8Vector(std::vector<bool> boolVector) {

	if (boolVector.empty()) {
		return std::vector<int8_t>();
	}

	std::vector<int8_t> int8Vector(boolVector.size());
	for (uint i=0 ; i<boolVector.size() ; i++) {
		int8_t value = (int8_t)boolVector.at(i);
		int8Vector.at(i) = value;
	}
	return int8Vector;
}

/**
 * Casts an int8 vector to boolean. This is necessary because HDF5 does not support
 * boolean data types, so they are transported from the worker to the application
 * and back as int8's.
 */
std::vector<bool> Hdf5Interface::int8ToBoolVector(std::vector<int8_t> int8Vector) {

	if (int8Vector.empty()) {
		return std::vector<bool>();
	}

	std::vector<bool> boolVector(int8Vector.size());
	for (uint i=0 ; i<int8Vector.size() ; i++) {
		bool value = (int8Vector.at(i) == (int8_t)1);
		boolVector.at(i) = value;
	}
	return boolVector;
}

void Hdf5Interface::openAndReadHdf5Group(Persistable& obj, hid_t hdf5ParentGroupId, std::string name) {
	hid_t hdf5GroupId = H5Gopen(hdf5ParentGroupId,name.c_str(), H5P_DEFAULT);
	if (!H5Aexists(hdf5GroupId, EMPTY_FIELD_ATT_NAME)) {
		obj.readHdf5(hdf5GroupId);
	}
	H5Gclose(hdf5GroupId);
}

void Hdf5Interface::createAndWriteHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
		std::string groupName, int32_t fieldOrder) {
	Hdf5Interface::createAndWriteHdf5Group(obj, hdf5ParentGroupId, groupName,
			false, fieldOrder, false);
}

void Hdf5Interface::createAndWriteParallelHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
		std::string groupName, bool isEmpty, int32_t fieldOrder) {
	Hdf5Interface::createAndWriteHdf5Group(obj, hdf5ParentGroupId, groupName,
			isEmpty, fieldOrder, true);
}

void Hdf5Interface::createAndWriteHdf5Group(const Persistable& obj, hid_t hdf5ParentGroupId,
		std::string groupName, bool isEmpty, int32_t fieldOrder, bool parallelFlag) {

	hsize_t dimsEmpty = 0;
	Hdf5Interface* hdf5Interface;
	if (isEmpty) {
		hdf5Interface = Hdf5Interface::Hdf5InterfaceForWriting(hdf5ParentGroupId,
				groupName, H5T_OPAQUE, 1, &dimsEmpty, HDF5_PERSISTABLE_TYPE_INT, false,
				fieldOrder) ;
	} else {
		hdf5Interface = Hdf5Interface::Hdf5InterfaceForWriting(hdf5ParentGroupId,
				groupName, H5T_OPAQUE, 1, NULL, HDF5_PERSISTABLE_TYPE_INT, false,
				fieldOrder) ;
	}
	if (parallelFlag) {
		hdf5Interface->addMarkerAttribute(PARALLEL_ARRAY_ATT_NAME);
	}
	obj.writeHdf5(hdf5Interface->get_hdf5GroupId());
	H5Gclose(hdf5Interface->get_hdf5GroupId());
}

void Hdf5Interface::addMarkerAttribute(char* attributeName) {
	hid_t attributeSpace = H5Screate(H5S_SCALAR);
	hid_t attributeId = H5Acreate(hdf5GroupId, attributeName,
			H5T_NATIVE_INT8, attributeSpace, H5P_DEFAULT, H5P_DEFAULT);
	H5Aclose(attributeId);
	H5Sclose(attributeSpace);
}

void Hdf5Interface::addScalarInt32Attribute(char* attributeName, int32_t attributeValue) {
	hid_t attributeSpace = H5Screate(H5S_SCALAR);
	hid_t attributeId = H5Acreate(hdf5GroupId, attributeName,
			H5T_NATIVE_INT32, attributeSpace, H5P_DEFAULT, H5P_DEFAULT);
	H5Awrite(attributeId, H5T_NATIVE_INT32, &attributeValue);
	H5Aclose(attributeId);
	H5Sclose(attributeSpace);
}


std::vector<std::string> Hdf5Interface::readStringArray() {

	if (datasetId == -1) {
		return std::vector<std::string>();
	}

	//	determine the size of the return array
	int nDims = this->getNDims();
	hsize_t* dims = this->getDims();
	hsize_t numel = 1;
	for (int iDim=0 ; iDim<nDims ; iDim++) {
		numel *= dims[iDim];
	}

	//	allocate the array of char*
	char* charArray[numel];

	//	read the array
	//	define the data type and read in
	hid_t hdf5Type = H5Tcopy(H5T_C_S1);
	H5Tset_size(hdf5Type, H5T_VARIABLE);
	H5Dread(datasetId, hdf5Type, H5S_ALL, H5S_ALL, H5P_DEFAULT, charArray);

	//	construct the string vector
	std::vector<std::string> stringVector(charArray, charArray + numel);
	return stringVector;

}

void Hdf5Interface::writeStringArray(std::vector<std::string> content) {

	//	transfer the contents of the string vector to an array of char* objects
	int nelem = content.size();
	char* contentArray[nelem];
	for (int i=0 ; i<nelem ; i++) {
		contentArray[i] = const_cast<char*>(content.at(i).c_str());
	}

	//	write to HDF5
	H5Dwrite(datasetId, datatypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, contentArray);

}

void Hdf5Interface::writeStringVector(hsize_t* offset, std::vector<std::string> content) {

	//	determine the size of the dataspace
	int nDims = this->getNDims();
	hsize_t* dims = this->getDims();

	//	set the hyperslab parameters -- we are taking a slab that is 1 unit in each dimension
	//	except for the last dimension, which is the size of the array in that dimension. For
	//	example, if the data is 3 x 4 x 5, each block will be 1 x 1 x 5.
	hsize_t stride[nDims];
	hsize_t count[nDims];
	hsize_t block[nDims];
	for (int i=0 ; i<nDims-1 ; i++) {
		stride[i] = 1;
		count[i] = 1;
		block[i] = 1;
	}
	stride[nDims-1] = 1;
	count[nDims-1] = 1;
	block[nDims-1] = dims[nDims-1];

	//	define the hyperslab and write it
	hid_t memspace = H5Screate_simple(nDims, block, NULL);
	H5Sselect_hyperslab(dataspaceId, H5S_SELECT_SET, offset, stride, count, block);
	int nElem = dims[nDims-1];

	const char** contentArray = new const char*[nElem];
	for (int i=0 ; i<nElem ; i++) {
		contentArray[i] = content.at(i).c_str();
	}
	H5Dwrite(datasetId, datatypeId, memspace, dataspaceId, H5P_DEFAULT, contentArray);

}

std::map<hid_t, int> Hdf5Interface::typeSizes = {
		{H5T_NATIVE_INT8, 1},
		{H5T_NATIVE_INT16, 2},
		{H5T_NATIVE_INT32, 4},
		{H5T_NATIVE_INT64, 8},
		{H5T_NATIVE_FLOAT, 4},
		{H5T_NATIVE_DOUBLE, 8},
		{H5T_C_S1, 1},
		{H5T_OPAQUE, 1}
};
