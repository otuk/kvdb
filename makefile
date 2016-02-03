# build kvdb library and tests

TARGET=test_kvdb

CC=gcc
CFLAGS = -std=c11 -g -Wall -Werror -O0
CFLAGS += -D_GNU_SOURCE # needed for ftruncate & MAP_FILE on linux
#CFLAGS += -fprofile-arcs -ftest-coverage
OBJECTS = $(TARGET).o kvdb.o hash_32.o
LDFLAGS =
LDLIBS = 

all: $(TARGET)

$(TARGET): $(OBJECTS)

clean:
	rm -f ./$(TARGET)
	rm -f ./*.o

# clean the test database created
cleand: clean
	rm -f ./*.db

memchk: all
	valgrind --dsymutil=yes --track-origins=yes --leak-check=full ./$(TARGET)

run:
	./$(TARGET)


# add install
# add debug differences
# add optimizer differences
#
