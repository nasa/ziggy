#ifndef IOHANDLER_H_
#define IOHANDLER_H_

#include <string>

#include <Persistable.h>

/**
 * Deserilizes Persistable inputs and serializes Persitable outputs.
 *
 * @author Todd Klaus
 */
class IOHandler {
public:

    IOHandler(const std::string& dir, const std::string& id, const std::string& binaryName);

    virtual ~IOHandler() {
    }

    void loadInputs(Persistable& inputs) const;
    void saveOutputs(Persistable& outputs) const;
    static bool fileExists(const std::string& name) ;

private:
    std::string dir;
    std::string id;
    std::string binaryName;
    std::string inputFilename;
    std::string outputFilename;
};

#endif /*IOHANDLER_H_*/
