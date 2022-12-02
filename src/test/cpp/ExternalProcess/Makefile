
OUTDIR=../../../../build/bin
OUT=$(OUTDIR)/testprog

INCLUDES=
CXXFLAGS=-g
CC=gcc
CXX=g++
LIBS= 
LDFLAGS=-g

SRC = testprog.cpp

OBJ = $(SRC:.cpp=.o)

all : $(OUT)

$(OUT): $(OBJ)
	mkdir -p $(OUTDIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ)

.cpp.o:
	$(CXX) $(INCLUDES) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(OUT) *~


