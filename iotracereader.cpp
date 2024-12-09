/***********************************************************************
 * This program is for reading the io-trace files. This provides an
 * interface to open, read next record, reset to the begining and close 
 * operations.
 ***********************************************************************/
#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>
//#include "dedupn.h"
#include "iotracereader.h"

using namespace std;

unsigned char IOTraceReader::hex2bin(char c) {
	if (c >= '0' && c <= '9') return (c - '0');
	if (c >= 'A' && c <= 'F') return (c - 'A' + 10);
	if (c >= 'a' && c <= 'f') return (c - 'a' + 10);
	printf("Invalid hex char : %x %c (%lu)\n", c, c, curRec);
	return 0;
}

IOTraceReader::IOTraceReader(char fname[100]) {
	strncpy(iotPath, fname, 99);
	curRec = 0;
}
IOTraceReader::IOTraceReader() {
	iotPath[0] = 0;
	curRec = 0;
	iotCount = 1;
}
int IOTraceReader::openTrace(unsigned int blk) {
	curRec = 0;
	iotCount = 1;
	iotCurrent = 0;
	iotBlockSize = blk;	
	ifs.open(iotPath);
	ifs >> IOTBaseTime;
	ifs.seekg(0L);
	iocomplete = 0;
	if (ifs.fail()) return -1;
	else return 0;
}
int IOTraceReader::openTrace(const char fname[100], unsigned int blk) {
	strncpy(iotPath, fname, 99);
	curRec = 0;
	iotCount = 1;
	iotCurrent = 0;
	iotBlockSize = blk;	
	ifs.open(iotPath);
	ifs >> IOTBaseTime;
	ifs.seekg(0L);
	iocomplete = 0;
	if (ifs.fail()) return -1;
	else return 0;
}

int IOTraceReader::openTrace(const char fname[][100], int n, unsigned int blk) {
	int i;

	if (n > 30) {
		cout << "IOTrace reader trace count is limited to 30 only\n";
		exit(0);
	}

	iotCount = n;
	iotCurrent = 0;

	for (i=0; i<n; i++) 
		strncpy(iotPaths[i], fname[i], 99);
	
	strncpy(iotPath, fname[0], 99);
	curRec = 0;
	iotBlockSize = blk;	
	ifs.open(iotPath);
	ifs >> IOTBaseTime;
	ifs.seekg(0L);
	iocomplete = 0;

	if (ifs.fail()) return -1;
	else return 0;
}
/*
int IOTraceReader::readNext(IORecord &rec) {
	char c1, c2;
	int i;

#ifndef	HOME
	char str[8*2*16+8];
#else
	int j;
#endif

	if (ifs.eof()) {
		// Go for the next trace
		cout << iotPath << ": trace has been completed\n";
		ifs.close();
		ifs.clear();
		iotCurrent++;

		if (iotCurrent < iotCount) {
			strncpy(iotPath, iotPaths[iotCurrent], 99);
			cout << "Opening trace " << iotPath << " ...\n";
			ifs.open(iotPath);
			if (ifs.fail()) {
				cout << iotPath << ": IOTrace opening failure\n\n";
				return 0;
			}
		}
		else return 0;
	}
	
	ifs >> rec.ior_ts >> rec.ior_pid >> rec.ior_process
		>> rec.ior_lba >> rec.ior_nb >> rec.ior_rw
		>> rec.ior_major >> rec.ior_minor;
#ifndef	HOME
	ifs >> str;
	if (ifs.fail()) return 0;
	for (i=0; i<16; i++) {
		c1 = str[2*i]; 
		c2 = str[2*i+1]; 
		rec.ior_md5hash[i] = (hex2bin(c1) << 4) | (hex2bin(c2));
	}
#else
	if (ifs.fail()) return 0;
	ifs.get(c1); // skip space 
	for (j=0; j<rec.ior_nb; j++) {
		for (i=0; i<16; i++) {
			ifs.get(c1);
			ifs.get(c2);

			rec.ior_md5hash[j][i] = (hex2bin(c1) << 4) | (hex2bin(c2));
		}
	}
#endif

	curRec++;
	return 1;
}

int IOTraceReader::readNextSegment(int *pid, int *op, unsigned long long int *ts1, unsigned long long int *ts2, struct segment *s) {
	struct IORecord rec1;
	int i;
	off_t offset;
	unsigned int nb;
#ifndef	HOME
	struct IORecord rec2;	// I/O Record
	int duphash;
	int j;
#endif
rept_readNext:	
	if (ifs.eof()) {
		// Go for the next trace
		cout << iotPath << ": trace has been completed\n";
		ifs.close();
		ifs.clear();
		iotCurrent++;

		if (iotCurrent < iotCount) {
			strncpy(iotPath, iotPaths[iotCurrent], 99);
			cout << "Opening trace " << iotPath << " ...\n";
			ifs.open(iotPath);
			if (ifs.fail()) {
				cout << iotPath << ": IOTrace opening failure\n\n";
				return 0;
			}
		}
		else return 0;
	}

	if (readNext(rec1) != 1) goto rept_readNext;
	nb = rec1.ior_nb;
	i = 1;
	*ts2 = *ts1 = rec1.ior_ts;
	//cout << "iorecord read next : " << rec1.ior_nb << endl;
	//cout << "size of s " << sizeof(struct segment) << endl;
#ifndef	HOME
	memcpy(s->seg_hash[i-1], rec1.ior_md5hash, 16);
	offset = ifs.tellg();

	while ((i < MAX_SEG_SIZE) && readNext(rec2)) {
		if ((rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			// Check if it is haing duplicate hash value
			duphash = 0;
			for (j=0; j<i; j++) {
				if (memcmp(s->seg_hash[j], rec2.ior_md5hash, 16) == 0) { 
					duphash = 1;
					break;
				}
			}
			if (duphash == 0) {
				// Not having duplicate hash
				i++;
				nb += rec2.ior_nb;
				memcpy(&(s->seg_hash[i-1]), rec2.ior_md5hash, 16);
				*ts2 = rec2.ior_ts;
				offset = ifs.tellg();
			}
			else {
				// Go back to this rec2
				ifs.seekg(offset, ios::beg);
				curRec--;
				break;
			}
		}
		else {
			// Go back to this rec2
			ifs.seekg(offset, ios::beg);
			curRec--;
			break;
		}
	}
	s->seg_size = i;

#else
	memcpy(s->seg_hash[i-1], rec1.ior_md5hash[0], nb*16);
	offset = ifs.tellg();
	s->seg_size = nb;
#endif
	s->seg_start = 	rec1.ior_lba;

	*pid = rec1.ior_pid;
	*op = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;
	
	return i;
}

int IOTraceReader::readFile(struct FileBlocks *fb) {
	struct IORecord rec1;
	//int i;
	off_t offset;
	unsigned int nb;
	struct IORecord rec2;	// I/O Record
	//int duphash;
#ifdef	HOME
	unsigned int j;
#endif
	struct HashList *hl;
rept_readNext:	
	if (ifs.eof()) {
		// Go for the next trace
		cout << iotPath << ": trace has been completed\n";
		ifs.close();
		ifs.clear();
		iotCurrent++;

		if (iotCurrent < iotCount) {
			strncpy(iotPath, iotPaths[iotCurrent], 99);
			cout << "Opening trace " << iotPath << " ...\n";
			ifs.open(iotPath);
			if (ifs.fail()) {
				cout << iotPath << ": IOTrace opening failure\n\n";
				return 0;
			}
		}
		else return 0;
	}

	if (readNext(rec1) != 1) goto rept_readNext;
	nb = rec1.ior_nb;
	//i = 1;
	fb->fb_ts1 = fb->fb_ts2 = rec1.ior_ts;
	// Add one hashlist record.
	if (fb->fb_hlist == NULL) {
		hl = fb->fb_hlist = (struct HashList *)malloc(sizeof(struct HashList));
		hl->hl_next = NULL;
	}
	else hl = fb->fb_hlist;

#ifndef	HOME
	memcpy(hl->hl_hash, rec1.ior_md5hash, 16);
	offset = ifs.tellg();

	while (readNext(rec2)) {
		if ((rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			//i++;
			if (hl->hl_next == NULL) {
				hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
				(hl->hl_next)->hl_next = NULL;
			}
			hl = hl->hl_next;
			nb += rec2.ior_nb;
			memcpy(hl->hl_hash, rec2.ior_md5hash, 16);
			fb->fb_ts2 = rec2.ior_ts;
			offset = ifs.tellg();
		}
		else {
			// Go back to this rec2
			ifs.seekg(offset, ios::beg);
			curRec--;
			break;
		}
	}
	//fb->fb_nb = i;
	fb->fb_nb = nb;
#else
	memcpy(hl->hl_hash, rec1.ior_md5hash[0], 16);
	for (j=1; j<nb; j++) {
		if (hl->hl_next == NULL) {
			hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
			(hl->hl_next)->hl_next = NULL;
		}
		hl = hl->hl_next;
		memcpy(hl->hl_hash, rec1.ior_md5hash[j], 16);
	}
	//i = nb;
	while (readNext(rec2)) {
		if ((rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			if (hl->hl_next == NULL) {
				hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
				(hl->hl_next)->hl_next = NULL;
			}
			hl = hl->hl_next;

			memcpy(hl->hl_hash, rec2.ior_md5hash[0], 16);
			for (j=1; j<nb; j++) {
				if (hl->hl_next == NULL) {
					hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
					(hl->hl_next)->hl_next = NULL;
				}
				hl = hl->hl_next;
				memcpy(hl->hl_hash, rec1.ior_md5hash[j], 16);
			}

			nb += rec2.ior_nb;
			//i += rec2.ior_nb;
			fb->fb_ts2 = rec2.ior_ts;
			offset = ifs.tellg();
		}
		else {
			// Go back to this rec2
			ifs.seekg(offset, ios::beg);
			curRec--;
			break;
		}
	}

	fb->fb_nb = nb;
#endif
	fb->fb_lbastart = rec1.ior_lba;
	fb->fb_rw = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;

	//return i;
	return nb;
}
*/

int IOTraceReader::reset() {
	curRec = 0;
	ifs.seekg(0L);
	if (ifs.fail()) return -1;
	else return 0;
}
unsigned int IOTraceReader::getCurCount() {
	return curRec;
}
int IOTraceReader::closeTrace() {
	curRec = 0;
	ifs.close();
	if (ifs.fail()) return -1;
	else return 0;
}

