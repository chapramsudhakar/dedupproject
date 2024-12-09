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

int IOTraceReaderHome::readNext(IORecordHome &rec) {
	char c1, c2;
	int i;
	int j;

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

	if (ifs.fail()) return 0;
	ifs.get(c1); // skip space 
	for (j=0; j<rec.ior_nb; j++) {
		for (i=0; i<16; i++) {
			ifs.get(c1);
			ifs.get(c2);

			rec.ior_md5hash[j][i] = (hex2bin(c1) << 4) | (hex2bin(c2));
		}
	}

	curRec++;
	lastIOTime = (rec.ior_ts - IOTBaseTime);

	return 1;
}

int IOTraceReaderHome::readNextSegment(int *pid, int *op, unsigned long long int *ts1, unsigned long long int *ts2, struct segmenthome *s) {
	struct IORecordHome rec1, rec2;
	int i;
	off_t offset;
	unsigned int nb;

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
	lastIOTime = (rec1.ior_ts - IOTBaseTime);

	//cout << "iorecord read next : " << rec1.ior_nb << endl;
	//cout << "size of s " << sizeof(struct segment) << endl;
	memcpy(s->seg_hash[i-1], rec1.ior_md5hash[0], nb*16);
	offset = ifs.tellg();
	while ((i < MAX_SEG_SIZE) && readNext(rec2)) {
		if ((rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			i++;
			nb += rec2.ior_nb;
			memcpy(&(s->seg_hash[i-1]), rec2.ior_md5hash, 16);
			*ts2 = (rec2.ior_ts - IOTBaseTime);
			lastIOTime = (rec2.ior_ts - IOTBaseTime);
			offset = ifs.tellg();
		}
		else {
			ifs.seekg(offset, ios::beg);
			curRec--;
			break;
		}
	}
	s->seg_size = nb;
	//s->seg_start = rec1.ior_lba + ((rec1.ior_major*40 + rec1.ior_minor)*1000000000ull*256);
	s->seg_start = rec1.ior_lba;

	*pid = rec1.ior_pid;
	*op = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;
	
	return i;
}

int IOTraceReaderHome::readFile(struct FileBlocks *fb) {
	struct IORecordHome rec1;
	int i;
	off_t offset;
	unsigned int nb;
	struct IORecordHome rec2;	// I/O Record
	//int duphash;
	unsigned int j;
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
	i = 1;
	fb->fb_ts1 = fb->fb_ts2 = (rec1.ior_ts - IOTBaseTime);
	lastIOTime = (rec1.ior_ts - IOTBaseTime);
	// Add one hashlist record.
	if (fb->fb_hlist == NULL) {
		hl = fb->fb_hlist = (struct HashList *)malloc(sizeof(struct HashList));
		assert(hl != NULL);
		hl->hl_next = NULL;
	}
	else hl = fb->fb_hlist;

	memcpy(hl->hl_hash, rec1.ior_md5hash[0], 16);
	for (j=1; j<nb; j++) {
		if (hl->hl_next == NULL) {
			hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
			assert(hl->hl_next != NULL);
			(hl->hl_next)->hl_next = NULL;
		}
		hl = hl->hl_next;
		memcpy(hl->hl_hash, rec1.ior_md5hash[j], 16);
	}
	i = nb;
	offset = ifs.tellg();
	while (readNext(rec2)) {
		if ((rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			if (hl->hl_next == NULL) {
				hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
				assert(hl->hl_next != NULL);
				(hl->hl_next)->hl_next = NULL;
			}
			hl = hl->hl_next;

			memcpy(hl->hl_hash, rec2.ior_md5hash[0], 16);
			for (j=1; j<rec2.ior_nb; j++) {
				if (hl->hl_next == NULL) {
					hl->hl_next = (struct HashList *)malloc(sizeof(struct HashList));
					assert(hl->hl_next != NULL);
					(hl->hl_next)->hl_next = NULL;
				}
				hl = hl->hl_next;
				memcpy(hl->hl_hash, rec1.ior_md5hash[j], 16);
			}

			nb += rec2.ior_nb;
			i += rec2.ior_nb;
			fb->fb_ts2 = (rec2.ior_ts - IOTBaseTime);
			lastIOTime = (rec1.ior_ts - IOTBaseTime);
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
	//fb->fb_lbastart = rec1.ior_lba + ((rec1.ior_major*40+rec1.ior_minor)*1000000000ull*256);
	fb->fb_lbastart = rec1.ior_lba;
	fb->fb_rw = (rec1.ior_rw == 'R') ? OP_READ : OP_WRITE;

	return i;
}

