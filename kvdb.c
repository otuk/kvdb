
#include <sys/types.h>  //open ftruncate
#include <sys/stat.h>  //open
#include <fcntl.h>  //open
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h> //mmap &munmap
#include <unistd.h>  // close(fd) ftruncate
#include <stdio.h> // FILE*

#include "_tnt_util.h" // common utilities
FILE* myerrorstream = NULL;  // this can be set to any stream

#include "kvdb.h"  //public header
#include "_kvdb_pr.h"  //private header
#include "hash_32.h" //FVN1A hash header for mimimum collison

/**
 *  KVDB's mapped file's fixed layout:
 *  1 * header (kvhdr_s),  
 *  size * hash-index of key-value locations (kidx_s), 
 *  stacksize * stack of removed location addresses (kidx_s), 
 *  dbsizefactor * key-value record entries (kv_s) 
**/

// offset constants used in accessing parts of mapped memory
static uint32_t OFFSET_KIDX;
static uint32_t OFFSET_STACK;
static uint32_t OFFSET_KV;
static uint32_t KVR_SIZE;

// db header contains all the configuration numbers db needs
static kvhdr_s* g_kv_hdr;


// initialize and write db header info to file
static void init_kvhdr(kvhdr_s* mdat, dbsize esize, uint16_t keysize, uint16_t valsize, uint32_t maxecount, uint16_t maxscount){
  g_kv_hdr = mdat;
  g_kv_hdr->esize = esize;
  g_kv_hdr->keysize = keysize;
  g_kv_hdr->valsize = valsize;
  g_kv_hdr->ecount = 0;
  g_kv_hdr->scount = 0;
  g_kv_hdr->maxecount = maxecount;
  g_kv_hdr->maxscount = maxscount;
}


// calculates the constants to be used for memory access
static void calculate_constants(dbsize size, uint16_t keysize, uint16_t valsize){
  //printf( "size of dbsize is %u\n", (uint32_t) sizeof(dbsize));
  //printf( "size of k index is %u\n", (uint32_t) sizeof(kidx_s));
  //printf( "size of kvhdr_s is %u\n", (uint32_t) sizeof(kvhdr_s));
  OFFSET_KIDX = sizeof(kvhdr_s);
  OFFSET_STACK = (size * sizeof(kidx_s)) + OFFSET_KIDX;
  OFFSET_KV = (DEL_STACK_FACTOR * size *  sizeof(kidx_s)) + OFFSET_STACK;
  KVR_SIZE =  sizeof(uint32_t)  //kv_s struct
            + sizeof(uint32_t)
            + (sizeof(char) * keysize)
            + (sizeof(char) * valsize);//kv_s struct
}


//initializes enveloping and opaque structures 
static kvdb_s* initialize_db_artifacts(const char* name, int fd, kvdata_s* mdat, uint16_t keysize, uint16_t valsize, dbsize size){
  kvdb_s* db_m = malloc(sizeof(kvdb_s));
  HANDLE(!db_m, 'y', return NULL, "Cannot allocate memory for kvdb_s.\n");
  //  int lerr = errno;
  //if(!db_m){
  //  EMSG(lerr, "Cannot allocate memory for kvdb_s.\n");
  //  return NULL;
  //}
  db_m->name = strdup(name);
  db_m->keysize = keysize;
  db_m->valsize = valsize;
  kvdata_s* data_m = malloc(sizeof(kvdata_s));
  int lerr = errno;
  if(!data_m){
    EMSG(lerr, "Cannot allocate memory for kvdb data_m.\n");
    return NULL;
  }
  db_m->data = data_m;   

  data_m->fd = fd;
  data_m->esize = size;  //?? why 0 in dgb
  data_m->mdat = mdat;
  return db_m;
}


/*
 *  create a new kvdb
 *
 *
 */
kvdb_s* create_kvdb(const char* name, dbsize size, uint16_t keysize, uint16_t valsize){  
  int fd = open(name, O_RDWR | O_CREAT | O_EXCL,  S_IRUSR | S_IWUSR);
  HANDLE(fd < 0, 0, return NULL,
	 "Cannot create file for kvdb_s for RW access %s\n", name);
  calculate_constants(size, keysize, valsize);
  size_t len = OFFSET_KV + ( DB_SIZE_FACTOR * size * KVR_SIZE);
  ftruncate(fd, len);

  void* mdat = mmap(NULL, len,
		    PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
  HANDLE(mdat == MAP_FAILED, 0, return NULL,
	 "Cannot map memory for kvdb data_m.\n");
  
  kvdb_s* db_m = initialize_db_artifacts(name, fd, mdat, keysize, valsize, size);
  kvdata_s* data_m= db_m->data;
  init_kvhdr(data_m->mdat, data_m->esize, keysize, valsize,
	     DB_SIZE_FACTOR * data_m->esize, DEL_STACK_FACTOR * data_m->esize );

  return db_m;  
}


// simply point the hdr pointer to the start of mapped memory
static void read_into_header(kvhdr_s* mdat){
  g_kv_hdr = mdat;
}


/*
 *  Load and existing kvdb with file name
 *
 *
 */
kvdb_s* load_kvdb(const char* name){
  int fd = open(name, O_RDWR,  S_IRUSR | S_IWUSR);
  HANDLE(fd < 0, 0, return NULL,
	 "Cannot load file for kvdb_s for RW access %s\n", name);
  struct stat sb;
  HANDLE(fstat(fd, &sb) == -1, 0, return NULL,
	 "Cannot read file stats %s\n", name);
  HANDLE(!S_ISREG (sb.st_mode), 0, return NULL,
	 "%s is not a normal file\n", name);

  void* mdat =  mmap(NULL, sb.st_size,
		     PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
  HANDLE(mdat == MAP_FAILED, 0, return NULL,
	 "Cannot map memory for kvdb data_m.\n");
  
  read_into_header(mdat);
  calculate_constants(g_kv_hdr->esize, g_kv_hdr->keysize, g_kv_hdr->valsize);
  kvdb_s* db_m = initialize_db_artifacts(name, fd, mdat, g_kv_hdr->keysize,
					 g_kv_hdr->valsize,  g_kv_hdr->esize);

  return db_m;  
}


/*
 *  disconnect from kvdb and claan up resources
 *
 *
 */
int disconnect_kvdb(kvdb_s* db_m){
  if(db_m){
    if(db_m->name){
      free((void*) db_m->name);
    }
    kvdata_s* data_m = (kvdata_s*) db_m->data; 
    if (data_m){
      if (data_m->mdat){
	munmap(data_m->mdat, data_m->esize);
	//msync()?
      }
      if(data_m->fd > 0){ 	
	close(data_m->fd);
      }
      free((void*)data_m);
    }
    free((void*) db_m);
  }
  return 1;
}


static void add_hdr_count(){
  g_kv_hdr->ecount++;
}

static uint32_t get_kidx_offset(uint32_t hashval){
  return (hashval*sizeof(kidx_s)) + OFFSET_KIDX;
}

static uint32_t get_kv_fa_offset(){
  // get the first available in kv_s region + offset
  if(g_kv_hdr->scount > 0){
    //use this previously used location
    // zeroout first
    // read  the last scount value
    return (g_kv_hdr->scount); // this is TBD
  }else{
    return (g_kv_hdr->ecount * KVR_SIZE ) + OFFSET_KV;
  }
}  

/*
static uint32_t get_kv_offset(uint32_t count){
  return (count*sizeof(KVR_SIZE)) + OFFSET_KV;
}
*/

static char* get_mdat(kvdb_s* db_m){
  kvdata_s* kvd = (kvdata_s*) db_m->data;
  return  kvd->mdat;
}

static uint32_t resize_hash(kvdb_s* db_m, uint32_t hash){
  //printf("has value %u, %X\n", hash, hash);
  kvdata_s* kvd = (kvdata_s*) db_m->data;
  //printf("esize %d\n",kvd->esize);
  hash %= kvd->esize;
  //printf("has value %u, %X\n", hash, hash);
  return hash;
}


static void write_kv(kvdb_s* db_m, kv_s* newkv, const char* key, const char* val){
  newkv->prev = 0;
  newkv->next = 0;
  strncpy((char*) newkv + 2*sizeof(uint32_t), key, db_m->keysize);
  strncpy((char*) newkv + 2*sizeof(uint32_t) + db_m->keysize, val, db_m->valsize);
}

static int already_populated(uint32_t hasval){
  return -1;  //todo ????
}


/*
 *  Add a new key - value pair to the db
 *
 *
 */
int add(kvdb_s* db_m, const char* key, const char* val){
  char* mdat = get_mdat(db_m);
  uint32_t hashval =  (uint32_t) fnv_32_str( (char*) key, FNV1_32_INIT);
  hashval = resize_hash(db_m, hashval);
  // preapare the new  kv record
  uint32_t kvloc = get_kv_fa_offset();  // write this to index
  kv_s* newkv = (kv_s*)(mdat + kvloc);
  write_kv(db_m, newkv, key, val);
  kidx_s* newidx = NULL;
  if(already_populated(hashval)>0){
    // to do
    return -1;//???
  }else{
    newidx =(kidx_s*) (mdat + get_kidx_offset(hashval));
    newidx->idx = kvloc; // kv val location
  }  
  
  //get_kv_fa offset - ie first available location
  // if get_kidx_offset value is empty write the get_the_kv offset there
  // if not trace the kv_s region until next ==0 and write the get+kthe_kv offset there
  
  add_hdr_count();   
  return 1;
}


static void read_kv(kvdb_s* db_m, kv_s* newkv, const char* key, char** val){

  // TODO same has val key differences 
  if(0 == strncmp(key, (char*) newkv + 2*sizeof(uint32_t),  db_m->keysize)){
    *val = malloc(sizeof(char)*db_m->valsize);
    strncpy(*val, (char*) newkv + 2*sizeof(uint32_t) + db_m->keysize,
	    db_m->valsize);
    //printf("Found key %s\n", key);
  }else{
    //printf("Did not find key %s\n", key);
  } 
}


/*
 *  Given a key get the value from db
 *
 *
 */
char* get(kvdb_s* db_m, const char* key){
  char* mdat = get_mdat(db_m);
  uint32_t hashval =  (uint32_t) fnv_32_str( (char*) key, FNV1_32_INIT);
  hashval = resize_hash(db_m, hashval);
  kidx_s* newidx = (kidx_s*) (mdat + get_kidx_offset(hashval));
  uint32_t kvloc = newidx->idx;
  //printf("index has kv loc  %u\n", kvloc);
  //uint32_t kvloc = get_kv_offset(count);  // write this to index
  // preapare the new  kv record  
  kv_s* newkv = (kv_s*)(mdat + kvloc);
  char* val = NULL;
  read_kv(db_m, newkv, key, &val);
  return val;
}


 
/** to do





int set(kvdb_s* db_m, const char* key, const char* val);
int del(kvdb_s* db_m, const char* key);

int has(kvdb_s* db_m, const char* key);
**/
