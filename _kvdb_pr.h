/**
   private header file for kvdb.c

 */

#ifndef _KVDB_PR_H
#define _KVDB_PR_H


// this is the opaque data representing the db
// besides a pointer to the mapped region
// it contains information on the file descriptor
// and the has maximum value  //??? is this duplicate is this ever used?
typedef struct{
  char* name;
  uint16_t keysize;
  uint16_t valsize;
  int fd;
  void* mdat;
} kvdata_s;


#define DB_SIZE_FACTOR 2    // allow twice the number of hash max
#define DEL_STACK_FACTOR 2  // same factor for stack so all elements can be deleted, without creating gaps in kv area

// db header
// this keeps information sufficient to reload the db
// after a program is done with using and disconnects
// the information is used for load method
typedef struct{
  const char fluff[128]; 
  dbsize hashsize;       // hash max size # of items
  uint16_t keysize;   // key size in # of chars
  uint16_t valsize;   // val  size in # of chars
  uint32_t ecount;    // CURRENT # of items in kv area 
  uint32_t scount;    // CURRENT # of items in del stack
  uint32_t maxecount; // maximum adds/elements before db runs out of space 
  uint32_t maxscount; // maximum deletions before del-stack runs out of space
  uint32_t flen; 
} kvhdr_s;


// the index record pointing to
// the location of the key-hashvalue in kv-record section
// this is in absolute address offset starting from the
// beginning of the mapped region
typedef struct{
  uint32_t idx; 
} kidx_s;

// kv record for 1 entry
// this is the linked list
// with prev and next
typedef struct{
  uint32_t  prev;
  uint32_t  next;
  char* key;
  char* val;
}kv_s;


#endif
