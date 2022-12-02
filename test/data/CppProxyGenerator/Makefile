
CSRCDIR=generated

# C++ compiler settings

INCLUDES = -I$(CSRCDIR) -I../../../../matlab/mi-common/include
CCFLAGS = -g -DDEBUG
LDFLAGS = -L../../../../matlab/mi-common/build/lib -lmicommon

CSRC = \
	$(CSRCDIR)/TestController.cpp \
	$(CSRCDIR)/main.cpp \
	$(CSRCDIR)/TestInputs.cpp \
	$(CSRCDIR)/TestOutputs.cpp \
	$(CSRCDIR)/PixelData.cpp

OBJ = $(CSRC:.cpp=.o)

all : test

test : $(CSRC)
	g++ $(CSRC) $(INCLUDES) $(CCFLAGS) $(LDFLAGS) -o test

clean :
	rm -f $(OBJ) test
	