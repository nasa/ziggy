#ifndef PERSISTABLE_H_
#define PERSISTABLE_H_

#include "hdf5.h"

/**
 * This is the base class of persistables.  Persistables objects that can be serialalized with the
 * persistable framework.
 */
 
class Persistable {
public:

    Persistable() {
    };

    virtual ~Persistable() {
    };

    virtual void readHdf5(hid_t hdf5Id) = 0;
    virtual void writeHdf5(hid_t hdf5Id) const = 0;
};

#endif /*PERSISTABLE_H_*/
