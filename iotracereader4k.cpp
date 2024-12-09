/***********************************************************************
 * This program is for reading the io-trace files. This provides an
 * interface to open, read next record, reset to the begining and close 
 * operations.
 ***********************************************************************/
#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
//#include "dedupn.h"
#include "iotracereader.h"

using namespace std;

int IOTraceReader4K::readNext(IORecord4K &rec) {
	char c1, c2;
	int i;
	//int j;
	char str[8*2*16+8];

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

	ifs >> str;
	if (ifs.fail()) return 0;
	for (i=0; i<16; i++) {
		c1 = str[2*i]; 
		c2 = str[2*i+1]; 
		rec.ior_md5hash[i] = (hex2bin(c1) << 4) | (hex2bin(c2));
	}

	curRec++;
	lastIOTime = (rec.ior_ts - IOTBaseTime);
	return 1;
}

int IOTraceReader4K::readNextSegment(int *pid, int *op, unsigned long long int *ts1, unsigned long long int *ts2, struct segment4k *s) {
	struct IORecord4K rec1;
	int i;
	off_t offset;
	unsigned int nb;
	struct IORecord4K rec2;	// I/O Record
	int duphash;
	int j;

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
				iocomplete = 1;
				return 0;
			}
		}
		else {
			iocomplete = 1;
			return 0;
		}
	}

	if (readNext(rec1) != 1) goto rept_readNext;
	nb = rec1.ior_nb;
	i = 1;
	*ts2 = *ts1 = (rec1.ior_ts - IOTBaseTime);
	//cout << "iorecord read next : " << rec1.ior_nb << endl;
	//cout << "size of s " << sizeof(struct segment) << endl;

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
				*ts2 = (rec2.ior_ts - IOTBaseTime);
				lastIOTime = (rec2.ior_ts - IOTBaseTime);
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

	//s->seg_start = 	rec1.ior_lba + ((rec1.ior_major*40 + rec1.ior_minor)*1000000000ull*256);
	s->seg_start = 	rec1.ior_lba;

	*pid = rec1.ior_pid;
	*op = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;
	
	return i;
}

int IOTraceReader4K::readFile(struct FileBlocks *fb) {
	struct IORecord4K rec1;
	//int i;
	off_t offset;
	unsigned int nb;
	struct IORecord4K rec2;	// I/O Record
	//int duphash;
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
				iocomplete = 1;
				return 0;
			}
		}
		else {
			iocomplete = 1;
			return 0;
		}
	}

	if (readNext(rec1) != 1) goto rept_readNext;
	nb = rec1.ior_nb;
	//i = 1;
	fb->fb_ts1 = fb->fb_ts2 = (rec1.ior_ts - IOTBaseTime);
	// Add one hashlist record.
	if (fb->fb_hlist == NULL) {
		hl = fb->fb_hlist = (struct HashList *)malloc(sizeof(struct HashList));
		assert(hl != NULL);
		hl->hl_next = NULL;
	}
	else hl = fb->fb_hlist;

	memcpy(hl->hl_hash, rec1.ior_md5hash, 16);
	offset = ifs.tellg();

	while (readNext(rec2)) {
		if ((rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			//i++;
			if (hl->hl_next == NULL) {
				hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
				assert(hl->hl_next != NULL);
				(hl->hl_next)->hl_next = NULL;
			}
			hl = hl->hl_next;
			nb += rec2.ior_nb;
			memcpy(hl->hl_hash, rec2.ior_md5hash, 16);
			fb->fb_ts2 = (rec2.ior_ts - IOTBaseTime);
			lastIOTime = (rec2.ior_ts - IOTBaseTime);
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
	//fb->fb_lbastart = rec1.ior_lba + ((rec1.ior_major*40 + rec1.ior_minor)*1000000000ull*256);
	fb->fb_lbastart = rec1.ior_lba;
	fb->fb_rw = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;

	//return i;
	return nb;
}

