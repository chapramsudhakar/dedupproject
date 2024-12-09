#ifndef __IOTRACEREADER_H__
#define __IOTRACEREADER_H__

using namespace std;
#include "dedupconfig.h"
#include "dedupn.h"


struct segmenthome {
	unsigned int	seg_start; 	// Segment starting block
	unsigned int 	seg_dev;	// Segment device number 
	unsigned char	seg_hash[1028][16];	// Each block's hash value
	unsigned short	seg_size;	// Segment size in terms of no of blocks
};

struct segment4k {
	unsigned int	seg_start; 	// Segment starting block
	unsigned int 	seg_dev;	// Segment device number
	unsigned char	seg_hash[MAX_SEG_SIZE][16];	// Each block's hash value
	unsigned short	seg_size;	// Segment size in terms of no of blocks
};

struct segment {
	unsigned int	seg_start; 	// Segment starting block
	unsigned int 	seg_dev;	// Segment device number
	unsigned char	seg_hash[MAX_SEG_SIZE][16];	// Each block's hash value
	unsigned short	seg_size;	// Segment size in terms of no of blocks
};

struct HashList {
	unsigned char 	hl_hash[16];
	struct HashList	*hl_next;
};

struct FileBlocks {
	unsigned long long int	fb_ts1;
	unsigned long long int	fb_ts2;
	unsigned int		fb_lbastart;
	unsigned int		fb_dev;
	unsigned int		fb_nb;
	unsigned int		fb_rw;
	struct HashList		*fb_hlist;
};

struct IORecordHome {
	unsigned long long int	ior_ts;		// Time stamp in nano seconds
	unsigned int 		ior_pid; 	// Process ID
	char 			ior_process[32];	// Process name
	unsigned int		ior_lba;	// Logical block address
	unsigned char 		ior_md5hash[1028][16];// MD5 hash value
	unsigned short 		ior_major;	// Major device number
	unsigned short 		ior_minor;	// Minor device number
	unsigned short 		ior_nb;		// Number blocks
	char 			ior_rw;		// Read / Wrire request
};

struct IORecord4K {
	unsigned long long int	ior_ts;		// Time stamp in nano seconds
	unsigned int 		ior_pid; 	// Process ID
	char 			ior_process[32];	// Process name
	unsigned int 		ior_lba;	// Logical block address
	unsigned char 		ior_md5hash[16];// MD5 hash value
	unsigned short 		ior_major;	// Major device number
	unsigned short 		ior_minor;	// Minor device number
	unsigned short 		ior_nb;		// Number blocks
	char 			ior_rw;		// Read / Wrire request
};

class IOTraceReader {
//private:
protected:
	char iotPaths[30][100];
	int iotCount;
	int iotCurrent;
	//ifstream ifs;			// Input file stream
	char iotPath[100];		// I/O trace file name
	unsigned long int curRec;	// Current record number
	unsigned char hex2bin(char c);

public:
	ifstream ifs;			// Input file stream
	unsigned long long lastIOTime;
	unsigned long long IOTBaseTime;
	unsigned int	iotBlockSize;
	unsigned int	iocomplete;

	IOTraceReader(char fname[100]);
	IOTraceReader();
	int openTrace(unsigned int blk);
	int openTrace(const char fname[100], unsigned int blk);
	int openTrace(const char fname[][100], int n, unsigned int blk);
	virtual int readNext(IORecord4K &rec) { return 0; };
	virtual int readNext(IORecordHome &rec) { return 0; };
	virtual int readNextSegment(int *pid, int *op, 
			unsigned long long int *ts1, 
			unsigned long long int *ts2, 
			struct segment4k *s) { return 0; };
	virtual int readNextSegment(int *pid, int *op, 
			unsigned long long int *ts1, 
			unsigned long long int *ts2, 
			struct segmenthome *s) { return 0; };
	virtual int readFile(struct FileBlocks *fb) { return 0; };
	int reset();
	unsigned int getCurCount();
	int closeTrace();
};

class IOTraceReader4K: public IOTraceReader { 
public:
	virtual int readNext(IORecord4K &rec);
	virtual int readNextSegment(int *pid, int *op, 
			unsigned long long int *ts1, 
			unsigned long long int *ts2, struct segment4k *s);
	virtual int readFile(struct FileBlocks *fb);
};

class IOTraceReaderHome: public IOTraceReader { 
public:
	virtual int readNext(IORecordHome &rec);
	virtual int readNextSegment(int *pid, int *op, 
			unsigned long long int *ts1, 
			unsigned long long int *ts2, struct segmenthome *s);
	virtual int readFile(struct FileBlocks *fb);
};

#endif


