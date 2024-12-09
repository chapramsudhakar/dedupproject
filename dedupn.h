#ifndef __DEDUP_H__
#define __DEDUP_H__

using namespace std;

#include "dedupconfig.h"

#define	DEDUPFULL	1
#define	DEDUPAPP	2
#define	DEDUPHDS	3
#define	DEDUPNATIVE	4



// Standard FIU traces and locally collected traces
// MAIL, WEB, HOME  (Unknown dedup category)
// LINUX, PPT, BOOKS (High dedup category)
// VIDIMG, NPTL (Low dedup category)
// Sizes of working set size in KB
const long int  MAIL_WSS = 58966824;
const long int  WEB_WSS = 2196696;
const long int  HOME_WSS = 18278797; //18170405;
const long int  LINUX_WSS = 345673556;
const long int  PPT_WSS = 19639564;
const long int  BOOKS_WSS = 19341840;
const long int  VIDIMG_WSS = 87355328;
const long int  NPTL_WSS = 135755076;


//#define	MAIL7WSS	58966824
//#define	WEB7WSS		2196696
//#define	HOME7WSS	18170405

#ifdef	MAIL
const long int	WSS = MAIL_WSS;
#else
#ifdef	WEB		
const long int	WSS = WEB_WSS;
#else
#ifdef	HOME		
const long int	WSS = HOME_WSS;
#else
#ifdef	LINUX
const long int	WSS = LINUX_WSS;
#else
#ifdef	BOOKPPT
const long int	WSS = (BOOKS_WSS + PPT_WSS);
#else
#ifdef	VIDIMG
const long int	WSS = (VIDIMG_WSS + NPTL_WSS);
#else
const long int	WSS = (1024*1024);
#endif
#endif
#endif
#endif
#endif
#endif

#define	HTYPE	1
#define LTYPE	2
#define UTYPE	4

#define MD5TIME4KB	30
#define MD5TIMEHALFKB	4
#define PKTCOMMTIME	120

//#define	FLUSH_TIME_30	1

#define	HASH_BUCK_COUNT		(1024 * 64)
#define MAX_BUCKETS		(1024 * 1024 * 4)
#define MAX_HBUCKETS		(1024 * 16 * 16)

#define	SMALL_FILE_BLKS	2
//#define MAX_SEG_SIZE		16
//#define MAX_DEDUP_SEG_SIZE	(4*MAX_SEG_SIZE)

#define MAX_PSEG_PER_BUCKET     340
#define MAX_TBUCKETS            (1024 * 1024 * 2)

#define OP_READ		1
#define OP_WRITE	2

#define FRAGMENT_THREASHOLD	3
#define DATADISK		0
#define METADATADISK		1

// Disk operation time calculation related macros 
#define NBLKS_PER_CYLINDER	(32768)
#define TRACK(blk)		(blk / NBLKS_PER_CYLINDER)
#define SEEKBASE		(10.0)
#define MINORSEEKTIME(blk1, blk2)	((abs((int)(TRACK(blk2) - TRACK(blk1))) * 0.00005))
#define ROTDELAY		(0.5 * (60000.0 / 10000.0))




// Bitmap related constants
#define 	BLOCKSIZE	4096
#define 	MAXBLOCKS	((1024 * 1024 * 256))
//#define 	MAXBLOCKS	(((4 * 1024 * 1024 / BLOCKSIZE) * 1024 * 1024))


#define OPERATION_READ		OP_READ
#define OPERATION_WRITE		OP_WRITE
#define OPERATION_NONE		3

#endif
