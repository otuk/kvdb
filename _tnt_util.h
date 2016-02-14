
#ifndef _TNT_UTIL_H
#define _TNT_UTIL_H

extern FILE* myerrorstream;
extern char abortset;

#define TNT_ERRMSG_SIZE 256

#define EMSG(ei, ...)							\
  do{									\
    if (! myerrorstream)						\
      myerrorstream = stderr;						\
    char msg[TNT_ERRMSG_SIZE]={0};					\
    char* rmsg = strerror_r(ei, msg, TNT_ERRMSG_SIZE);			\
    fprintf(myerrorstream,"%s:%d-ERR:%d %s\n", __FILE__, __LINE__, ei, rmsg); \
    fprintf(myerrorstream, __VA_ARGS__ );				\
  }while(0);								\


#define HANDLE(condition, isabort, action, ...) if((condition)){	\
    EMSG(errno, __VA_ARGS__);						\
    if ((isabort))							\
      exit(EXIT_FAILURE); 					\
    else{								\
      do{								\
	action ;							\
      }while(0);							\
    }									\
  }


#endif
