#
# Makefile for the testmi algorithm binary
#

BINARY_NAME := testmi

GENERATED_SOURCE_DIR := ../../../../build/src/cpp/testmi
OUTPUT_DIR_OBJ := ../../../../build/obj
OUTPUT_DIR_BIN := ../../../../build/bin

PERSISTABLE_LIBDIR := ../../../../build/lib
PERSISTABLE_LIBNAME := ziggymi
PERSISTABLE_INC := ../../../../src/main/cpp/libziggymi/

CPP_FILES := $(wildcard $(GENERATED_SOURCE_DIR)/*.cpp)
CPP_FILES += $(wildcard ./*.cpp) 
OBJ_FILES := $(addprefix $(OUTPUT_DIR_OBJ)/,$(notdir $(CPP_FILES:.cpp=.o)))

INCLUDES :=. $(GENERATED_SOURCE_DIR) $(PERSISTABLE_INC)
INC_PARAMS=$(foreach d, $(INCLUDES), -I$d)

CC_FLAGS := -Wall $(INC_PARAMS) -std=c++11 -O2 -DDEBUG
LD_FLAGS := -L$(PERSISTABLE_LIBDIR) -l$(PERSISTABLE_LIBNAME)

VPATH = .:$(GENERATED_SOURCE_DIR)

#all : $(BINARY_NAME)

$(BINARY_NAME): $(OBJ_FILES)
	g++ $(LD_FLAGS) -o $@ $^

$(OUTPUT_DIR_OBJ)/%.o: %.cpp
	mkdir -p $(OUTPUT_DIR_OBJ)
	g++ $(CC_FLAGS) -c -o $@ $<
	
clean:
	rm -f $(OUTPUT_DIR_OBJ)/* $(BINARY_NAME)
	
