
#include <sys/types.h>  //open ftruncate
#include <sys/stat.h>  //open
#include <fcntl.h>  //open
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h> //mmap &munmap
#include <unistd.h>  // close(fd) ftruncate


#include "kvdb.h"
#include "hash_32.h"

#define EMSG(ei, fen, ...)				\
  do{							\
    char* msg = strerror(ei);				\
    fprintf(stderr, "ERR:%d %s\n", ei, msg);		\
    fprintf(stderr, fen, __VA_ARGS__ );			\
    free(msg);						\
  }while(0)						\

typedef struct{
  int fd;
  dbsize esize;
  void* mdat;
} kvdata_s;

#define DB_SIZE_FACTOR 2
#define DEL_STACK_SIZE 100

typedef struct{
  dbsize esize;
  uint16_t keysize;
  uint16_t valsize;
  uint32_t ecount;
  uint32_t scount;
  uint32_t maxecount;
  uint32_t maxscount;
} kvhdr_s;
 
typedef struct{
  uint32_t idx; 
} kidx_s;

typedef struct{
  uint32_t  prev;
  uint32_t  next;
  char* key;
  char* val;
}kv_s;

/**
   file layout:
   kvhdr_s,  
   size*kidx_s,
   stacksize*kidx_s, 
   dbsizefactor*kv_s data size
*/

static uint32_t OFFSET_KIDX;
static uint32_t OFFSET_STACK;
static uint32_t OFFSET_KV;
static uint32_t KVR_SIZE;

static kvhdr_s* kv_hdr;

void init_kvhdr(kvhdr_s* mdat, dbsize esize, uint16_t keysize, uint16_t valsize, uint32_t maxecount, uint16_t maxscount){
  kv_hdr = mdat;
  kv_hdr->ecount = 0;
  kv_hdr->esize = esize;
  kv_hdr->keysize = keysize;
  kv_hdr->valsize = valsize;
  kv_hdr->scount = 0;
  kv_hdr->maxecount = maxecount;
  kv_hdr->maxscount = maxscount;
}


kvdb_s* create_kvdb(const char* name, dbsize size, uint16_t keysize, uint16_t valsize){
  
  int fd = open(name, O_RDWR | O_CREAT | O_EXCL,  S_IRUSR | S_IWUSR);
  int lerr = errno;
  if (fd < 0){
    EMSG(lerr, "Cannot open/create file for kvdb_s for RW access %s\n", name);
    return NULL;
  }  
  printf( "size of %u\n", sizeof(kidx_s));
  OFFSET_KIDX = sizeof(kvhdr_s);
  OFFSET_STACK=  (size * sizeof(kidx_s)) + OFFSET_KIDX;
  OFFSET_KV = (DEL_STACK_SIZE * sizeof(kidx_s)) + OFFSET_STACK;
  KVR_SIZE =  sizeof(uint32_t)  //kv_s struct
            + sizeof(uint32_t)
            + (sizeof(char) * keysize)
            + (sizeof(char) * valsize);//kv_s struct
  size_t len = OFFSET_KV + ( DB_SIZE_FACTOR * size * KVR_SIZE);
			
  ftruncate(fd, len);
  
  kvdb_s* db = malloc(sizeof(kvdb_s));
  lerr = errno;
  if(!db){
    EMSG(lerr, "Cannot allocate memory for %s.\n", "kvdb_s");
    return NULL;
  }

  db->name = strdup(name);
  db->keysize = keysize;
  db->valsize = valsize;

  kvdata_s* data = malloc(sizeof(kvdata_s));
  lerr = errno;
  if(!data){
    EMSG(lerr, "Cannot allocate memory for %s data.\n", "kvdb_s");
    return NULL;
  }

  db->data = data;   
  data->fd = fd;
  data->esize = size;  //?? why 0 in dgb
  
  data->mdat = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
  lerr = errno;
  if(data->mdat == MAP_FAILED){
    EMSG(lerr, "Cannot map memory for %s.\n", "kvdb_s");
    return NULL;
  }

  init_kvhdr(data->mdat, data->esize, keysize, valsize, DB_SIZE_FACTOR, DEL_STACK_SIZE);
  
  return db;  
}


int free_kvdb(kvdb_s* db){
  free((void*) db->name);
  kvdata_s* data = (kvdata_s*) db->data; 
  munmap(data->mdat, data->esize);
  //msync()?
  close(data->fd);
  free((void*)data);
  free((void*) db);
  return 0;
}

void add_hdr_count(){
  kv_hdr->ecount++;
}

uint32_t get_kidx_offset(uint32_t hashval){
  return hashval*sizeof(kidx_s) + OFFSET_KIDX;
}

uint32_t get_kv_fa_offset(kidx_s idx){
  // get the first available in kv_s region + offset
  if(kv_hdr->scount > 0){
    //use this previously used location
    // zeroout first
  }else{
    return (kv_hdr->ecount * KVR_SIZE ) + OFFSET_KV;
}

static char* get_mdat(kvdb_s* db){
  kvdata_s* kvd = (kvdata_s*) db->data;
  return  kvd->mdat;
}

static uint32_t resize_hash(kvdb_s* db, uint32_t hash){
  printf("has value %u, %X\n", hash, hash);
  kvdata_s* kvd = (kvdata_s*) db->data;
  printf("esize %d\n",kvd->esize);
  printf("has value %u, %X\n", hash, hash);
  return hash %= kvd->esize;
}

 
int add(kvdb_s* db, const char* key, const char* val){
  char* mdat = get_mdat(db);
  uint32_t hashval =  (uint32_t) fnv_32_str( (char*) key, FNV1_32_INIT);
  hashval = resize_hash(db, hashval);
  // preapare the new  kv record
  kv_s* newkv = (mdat + get_kv_fa_offset(kidx_s idx));
  newkv->prev =0;
  newkv->next =0;
  newkv->key = newkv + 2*sizeof(uint32_t);
  newkv->value = (new->key) + 
  init_kv(newkv);

  kidx_s* idx = NULL;
  if(already_populated(hashval)){
    idx = get_kv_
  }else{
  }  
  
  
  //(mdat + get_kidx_offset(hashval) // index of next address
  //get_kv_fa offset - ie first available location
  // if get_kidx_offset value is empty write the get_the_kv offset there
  // if not trace the kv_s region until next ==0 and write the get+kthe_kv offset there
  add_hdr_count();
   
  return 1;
}


/** to do

kvdb_s* load_kvdb(const char* name){
}




int set(kvdb_s* db, const char* key, const char* val);
int del(kvdb_s* db, const char* key);
int get(kvdb_s* db, const char* key, char* buf, size_t size);
int has(kvdb_s* db, const char* key);
**/
