#ifndef __WRITEBUF_H__
#define __WRITEBUF_H__

#include "dedupconfig.h"
#include "dedupn.h"
#include "cachesize.h"

struct WriteReqBuf {
	unsigned long long int 	wr_ts;
	unsigned int		wr_lba;
	unsigned char 		wr_md5hash[16];
	struct WriteReqBuf	*wr_tsnext;
	struct WriteReqBuf	*wr_tsprev;
	struct WriteReqBuf	*wr_lbanext;
	struct WriteReqBuf	*wr_lbaprev;
	struct WriteReqBuf	*wr_hashnext;
	struct WriteReqBuf	*wr_hashprev;
	unsigned int		dummy1;
	unsigned int		dummy2;
	unsigned int		dummy3;
};

#define	WRBMCOUNT	(WRB_CACHE_SIZE/sizeof(WriteReqBuf))

void insertWriteReqTSHead(WriteReqBuf *wreq);
void insertWriteReqTS(WriteReqBuf * wreq);
void deleteWriteReqTS(WriteReqBuf *wreq);
void insertWriteReqHASH(WriteReqBuf *wreq);
void deleteWriteReqHASH(WriteReqBuf *wreq);
WriteReqBuf * searchWriteReq(unsigned int lba);
WriteReqBuf * searchWriteReqNear(unsigned int lba);
void insertWriteReqLBA(WriteReqBuf *wreq);
void deleteWriteReqLBA(WriteReqBuf *wreq);
void displayWriteReqBufMemStat(void);

#endif
