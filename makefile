# build kvdb and kvdb_test

TARGET=test_kvdb

CC=gcc
OPTIMIZE = -O0
prod: OPTIMIZE = -O3
PROFILE =
prof: PROFILE = -pg # for gprof profiling
CFLAGS = $(PROFILE) -std=c11 -g -Wall -Werror $(OPTIMIZE) 
CFLAGS += -D_GNU_SOURCE # needed for ftruncate & MAP_FILE on linux
#CFLAGS += -fprofile-arcs -ftest-coverage
OBJECTS = $(TARGET).o kvdb.o hash_32.o
LDFLAGS =  $(PROFILE)  
LDLIBS = 



all: $(TARGET)

prod: all

prof: cleand all
	./$(TARGET)
	gprof ./$(TARGET) > kvdb.profile
	cat ./kvdb.profile | less


$(TARGET): $(OBJECTS)

clean:
	rm -f ./$(TARGET)
	rm -f ./*.o


# clean the test database created
cleand: clean
	rm -f ./kv001.db
	rm -f ~/kv001.db
	rm -f ./gmon.out
	rm -f ./*.profile

memchk: all
	valgrind --dsymutil=yes --track-origins=yes \
                 --leak-check=full ./$(TARGET)

run:
	./$(TARGET)



