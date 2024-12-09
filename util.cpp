
#include <iostream>
#include <iomanip>
#include <fstream>
#include <time.h>
#include <math.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <assert.h>
#include "dedupn.h"
//#include "writebuf.h"
//#include "iotracereader.h"
#include "cache.h"
#include "util.h"

using namespace std;
/************************************************/

extern unsigned long long int DISK_READ_COUNT[2];
extern unsigned long long int DISK_READ_BCOUNT[2];
extern unsigned long long int DISK_WRITE_COUNT[2];
extern unsigned long long int DISK_WRITE_BCOUNT[2];
extern double DISK_READ_TOT_TIME[2];		// Time in mille seconds
extern double DISK_WRITE_TOT_TIME[2];
extern double DISK_READ_MIN_TIME[2];		// Time in mille seconds
extern double DISK_READ_MAX_TIME[2];
extern double DISK_WRITE_MIN_TIME[2];		// Time in mille seconds
extern double DISK_WRITE_MAX_TIME[2];
//extern unsigned long int DISK_READ_REQ_COUNT[1028][2000];
//extern unsigned long int DISK_WRITE_REQ_COUNT[1028][2000];
//extern unsigned long int DISK_IWRITE_REQ_COUNT[1028][2000];
extern unsigned long long int READ_REQ_COUNT;
extern unsigned long long int WRITE_REQ_COUNT;

extern unsigned long int prevblock[2];		// Two disks

extern double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
extern unsigned long int r_n[1028], w_n[1028];
extern double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
extern double w_flush;

unsigned long int CLOCK_PREV, CLOCK_MICRO;
unsigned long int DISK_MILLE, DISK_PREV1, DISK_PREV2;
/************************************************/

//extern ofstream dsim;

// Assigning hash values
void assign_md5hash(void * dest, const void * src) {
	memcpy(dest, src, 16);
}

int compare_md5hash(const void * h1, const void * h2) {
	return memcmp(h1, h2, 16);
}

void assign_uint(void * dest, const void * src) {
	memcpy(dest, src, sizeof(unsigned int));
}

int compare_uint(const void * v1, const void * v2) {
	unsigned int val1, val2;
	val1 = *(unsigned int *)v1;
	val2 = *(unsigned int *)v2;

	return (val1 - val2);
}

int md5hashIndex(void * h, unsigned int HASH_SIZE) {
	unsigned int x;

	memcpy(&x, h, sizeof(unsigned int));
	return (x % HASH_SIZE);
}

// In terms of 512 byte blocks and count of such blocks
static double optime(int disk, unsigned long long int block, unsigned int count, unsigned long long int ts, unsigned long long int lastoptime[]) {
	//static unsigned long long int lastoptime[2] = {0, 0};
	double t;
	unsigned long long int timgap;
	unsigned long long int trgap;

	timgap = (ts - lastoptime[disk]) / (1000 * 1000); // Milliseconds
	trgap = (TRACK(prevblock[disk]) >= TRACK(block)) ? (TRACK(prevblock[disk]) - TRACK(block)) : (-(TRACK(prevblock[disk]) - TRACK(block)));

	if (timgap > 1000) t = 4 * SEEKBASE;
	else if (trgap > (1024 * 1024)) t = SEEKBASE;
	else if (trgap > (1024 * 256)) t = 0.8 * SEEKBASE;
	else if (trgap > (1024 * 32)) t = 0.7 * SEEKBASE;
	else if (trgap > 1024) t = 0.5 * SEEKBASE;
	else if (trgap > 0) t = 0.4 * SEEKBASE;
	else t = 0;

	t += MINORSEEKTIME(prevblock[disk], block) + ROTDELAY + count * 0.09;
	lastoptime[disk] = ts+t;

	return t;
}

// In terms of 512 byte blocks and count of such blocks
void DiskRead(StorageNode *sn, unsigned long long int ts, int disk, unsigned long long int block, unsigned int count) {
	double t;
	//int ind;

	//if (!((disk >= 0) && (count > 0) && (count <= (8*MAX_SEG_SIZE)))) {
//#ifdef	__DEBUG__
		//cout << "DiskRead : disk " << disk << " blk " << block << " count " << count << " max " << 8*MAX_SEG_SIZE << endl;
//#endif
		//assert((disk >= 0) && (count > 0) && (count <= (8*MAX_SEG_SIZE)));
		
	//}
	sn->DISK_READ_COUNT[disk]++;
	sn->DISK_READ_BCOUNT[disk] += count;

	// Disksim trace output
	// Request time (ms) deviceno block number request size request type (read = 1/ write = 0)
	t = ts;
	
	//dsim << t / 1000000.0 << "\t" << disk+1 << "\t" << block << "\t" << count << "\t1" << endl;

	t = optime(disk, block, count, ts, sn->lastoptime); 
	
	sn->DISK_READ_TOT_TIME[disk] += t;
	sn->prevblock[disk] = block + count - 1;
	sn->lastoptime[disk] = ts + t;
}

// In terms of 512 byte blocks and count of such blocks
void DiskWrite(StorageNode *sn, unsigned long long int ts, int disk, unsigned long long int block, unsigned int count) {
	double t;

	//if (!((disk >= 0) && (count > 0) && (count <= (8*MAX_SEG_SIZE)))) {
//#ifdef	__DEBUG__
		//cout << "DiskWrite : disk " << disk << " blk " << block << " count " << count << " max " << 8*MAX_SEG_SIZE << endl;
//#endif
		//assert((disk >= 0) && (count > 0) && (count <= (8*MAX_SEG_SIZE)));
	//}
	sn->DISK_WRITE_COUNT[disk]++;
	sn->DISK_WRITE_BCOUNT[disk] += count;

	// Disksim trace output
	// Request time (ms) deviceno block number request size request type (read = 1/ write = 0)
	t = ts;

	//dsim << t/1000000.0 << "\t" << disk+1 << "\t" << block << "\t" << count << "\t0" << endl;
	t = optime(disk, block, count, ts, sn->lastoptime); 
	sn->DISK_WRITE_TOT_TIME[disk] += t;
	sn->prevblock[disk] = block + count - 1;
	sn->lastoptime[disk] = ts + t;
}

// Find the minimum hash value among the list of hashes
unsigned char * minHash(unsigned char list[][16], int count) {
	int i, m;

	m = 0;	// Assume the first one as min hash
	for (i=1; i<count; i++) 
		if (memcmp(list[i], list[m], 16) < 0) m = i;

	return list[m];
}

unsigned char * minHash2(WriteReqBuf * s, int count) {
	int i;
	WriteReqBuf *t;
	unsigned char *m;

	m = s->wr_md5hash;	// Assume the first one as min hash
	t = s->wr_tsnext;
	for (i=1; i<count; i++) {
		if (memcmp(t->wr_md5hash, m, 16) < 0) m = t->wr_md5hash;
		t = t->wr_tsnext;
	}

	return m;
}

unsigned char * minHash3(HashList * l, int count) {
	int i;
	HashList *t;
	unsigned char *m;

	m = l->hl_hash;	// Assume the first one as min hash
	t = l->hl_next;
	for (i=1; i<count; i++) {
		if (memcmp(t->hl_hash, m, 16) < 0) m = t->hl_hash;
		t = t->hl_next;
	}

	return m;
}

/*
void displayIORecord(IORecord &rec) {
        int i;
#ifdef  HOME
        int j;
#endif

        cout << dec << "TS\t\t= " << rec.ior_ts
                << "\nPID\t\t= " << rec.ior_pid
                << "\nLBA\t\t= " << rec.ior_lba
                << "\nNumBlocks\t= " << rec.ior_nb
                << "\nRead/Write\t= " << rec.ior_rw
                << "\nMD5\t\t= ";
#ifndef HOME
                for (i=0; i<16; i++)
                        cout << hex << setw(2) << (unsigned int)rec.ior_md5hash[i];
#else
                for (j=0; j<8; j++)
                        for (i=0; i<16; i++)
                                cout << hex << setw(2) << (unsigned int)rec.ior_md5hash[j][i];
#endif
                cout << dec << endl;
}
*/

void displayIORecord(IORecord4K &rec) {
	int i;

	cout << dec << "TS\t\t= " << rec.ior_ts 
		<< "\nPID\t\t= " << rec.ior_pid 
		<< "\nLBA\t\t= " << rec.ior_lba 
		<< "\nNumBlocks\t= " << rec.ior_nb 
		<< "\nRead/Write\t= " << rec.ior_rw
		<< "\nMD5\t\t= ";
		for (i=0; i<16; i++) 
			cout << hex << setw(2) << (unsigned int)rec.ior_md5hash[i];
		cout << dec << endl;
}

void displayIORecord(IORecordHome &rec) {
	int i;
	int j;

	cout << dec << "TS\t\t= " << rec.ior_ts 
		<< "\nPID\t\t= " << rec.ior_pid 
		<< "\nLBA\t\t= " << rec.ior_lba 
		<< "\nNumBlocks\t= " << rec.ior_nb 
		<< "\nRead/Write\t= " << rec.ior_rw
		<< "\nMD5\t\t= ";
		for (j=0; j<8; j++) 
			for (i=0; i<16; i++) 
				cout << hex << setw(2) << (unsigned int)rec.ior_md5hash[j][i];
		cout << dec << endl;
}

int readNextSegment(IOTraceReader *iot[], int iotcount, 
		int *pid, int *op, unsigned long long int *ts1, 
		unsigned long long int *ts2, 
		segment4k *s1, segmenthome *s2, int *trno) {
	int i, sel, retval;

	// Find the first not completed io trace
readnextsegment_1:
	sel = 0;
	while ((sel < iotcount) && (iot[sel]->iocomplete)) 
		sel++;
	if (sel >= iotcount) return 0;

	// Find the minimum lastIOTime
	for (i=sel+1; i<iotcount; i++)
		if ((!(iot[i]->iocomplete)) && 
			(iot[i]->lastIOTime < iot[sel]->lastIOTime)) 
			sel = i;

	if (iot[sel]->iotBlockSize == 512)
		retval = iot[sel]->readNextSegment(pid, op, ts1, ts2, s2);
	else 
		retval = iot[sel]->readNextSegment(pid, op, ts1, ts2, s1);

	if (retval == 0) goto readnextsegment_1;

	*trno = sel;

	return retval;
}

int readNextFile(IOTraceReader *iot[], int iotcount, 
		struct FileBlocks *fb, int *trno) {
	int i, sel = 0, retval;

	// Find the first not completed io trace
readnextfile_1:
	sel = 0;
	while ((sel < iotcount) && (iot[sel]->iocomplete)) 
		sel++;
	if (sel >= iotcount) return 0;

	// Find the minimum lastIOTime
	for (i=sel+1; i<iotcount; i++)
		if ((!(iot[i]->iocomplete)) && 
			(iot[i]->lastIOTime < iot[sel]->lastIOTime)) 
			sel = i;

	retval = iot[sel]->readFile(fb);

	if (retval == 0) goto readnextfile_1;

	*trno = sel;

	return retval;
}
/*
void display_iwrite_response() {
	unsigned int i, j;
	unsigned long int count, tcount, gtcount, tmin, tmax;
	double wsum, gwsum, avg, gavg, var, stdev;
	double avgseg, sumseg;

	gtcount = 0;
	gwsum = 0;
	sumseg = 0;
	cout << "ImmWrite \tblkcount \tavg \tstdev \tmin \tmax \treqs\n";

	for (i=0; i<1028; i++) {
		wsum = 0.0;
		count = tcount = 0;
		tmin = (DISK_IWRITE_REQ_COUNT[i][0] > 0) ? 0 : 10000;
		tmax = 0;

		for (j=0; j<2000; j++) {
			tcount += DISK_IWRITE_REQ_COUNT[i][j];
			wsum += (DISK_IWRITE_REQ_COUNT[i][j] * j);
			if (DISK_IWRITE_REQ_COUNT[i][j] > 0) {
				if (tmin > j) tmin = j;
				if (tmax < j) tmax = j;
			}
			sumseg += (DISK_IWRITE_REQ_COUNT[i][j] * (i+1));
		}

		if (tcount > 0) {
			avg = wsum / tcount;
			var = 0.0;
			for (j=0; j<2000; j++) {
				count = DISK_IWRITE_REQ_COUNT[i][j];
				var += (((j - avg) * (j - avg)) * count);
			}
			stdev = sqrt(var / tcount);
			cout << "\t" << i+1 << "\t\t" << avg << "\t" << stdev << "\t" << tmin << "\t" << tmax << "\t" << tcount << endl;
		}
		gtcount += tcount;
		gwsum += wsum;
	}

	// Stdev
	gavg = gwsum / gtcount;
	avgseg = sumseg / gtcount;
	var = 0.0;

	for (i=0; i<1028; i++) {
		for (j=0; j<2000; j++) {
			count = DISK_IWRITE_REQ_COUNT[i][j];
			var += (((j - gavg) * (j - gavg)) * count);
		}
	}

	stdev = sqrt(var / gtcount);

	cout << "Total disk iwrite requests : " << gtcount
		<< " gavg2 " << gavg 
		<< " stdev " << stdev 
		<< "\n Average write segment length " << avgseg << endl;
}
*/
/*
void display_io_response(void) {
	unsigned int i;
	unsigned long int gtcount;
	double gwsum, gavg, var, stdev;
	double avgseg, sumseg;

	cout << "Total read requests : " << READ_REQ_COUNT << endl
		<< "Total write requests : " << WRITE_REQ_COUNT << endl;
	cout << "Read \tblkcount \tavg(ms) \tstdev \tmin(ms) \tmax(ms) \treqs\n";
	gtcount = 0;
	gwsum = 0;
	sumseg = 0;
	for (i=0; i<1028; i++) {

		// Exclude zero units of time, which are served through
		// cached copies.
		if (r_n[i] > 0) {
			stdev = sqrt(r_var[i]);
			cout << "\t" << i+1 << "\t\t" << r_mean[i]/1000 << "\t" << stdev/1000 << "\t" << r_min[i]/1000 << "\t" << r_max[i]/1000 << "\t" << r_n[i] << endl;
		}
		gtcount += r_n[i];
		gwsum += (r_mean[i] * r_n[i]);
		sumseg += ((i+1) * r_n[i]);
	}
	// Stdev
	gavg = gwsum / gtcount;
	avgseg = sumseg / gtcount;
	var = 0.0;

	for (i=0; i<1028; i++) 
		var += (r_var[i] * r_n[i]);

	stdev = sqrt(var / gtcount);

	cout << "Total disk read requests : " << gtcount << " gavg1 " 
		<< (DISK_READ_TOT_TIME[0] / gtcount) << " gavg2 " 
		<< gavg/1000 << " stdev " << stdev/1000 
		<< "\n Average read segment length " << avgseg/2 << "KB" << endl;
	cout << "\nAverage read throughput : " 
		<< (sumseg * 512 * 1000000) / (1024 * 1024) / gwsum 
		<< "MB / Sec\n\n";
	gtcount = 0;
	gwsum = 0;
	sumseg = 0;
	cout << "Write \tblkcount \tavg(ms) \tstdev \tmin(ms) \tmax(ms) \treqs\n";

	for (i=0; i<1028; i++) {

		if (w_n[i] > 0) {
			stdev = sqrt(w_var[i]);
			cout << "\t" << i+1 << "\t\t" << w_mean[i]/1000 << "\t" << stdev/1000 << "\t" << w_min[i]/1000 << "\t" << w_max[i]/1000 << "\t" << w_n[i] << endl;
		}
		gtcount += w_n[i];
		gwsum += (w_mean[i] * w_n[i]);
		sumseg += ((i+1) * w_n[i]);
	}

	// Stdev
	gavg = gwsum / gtcount;
	avgseg = sumseg / gtcount;
	var = 0.0;

	for (i=0; i<1028; i++) 
		var += (w_var[i] * w_n[i]);

	stdev = sqrt(var / gtcount);

	cout << "Total disk write requests : " << gtcount << " gavg1 " 
		<< (DISK_WRITE_TOT_TIME[0] / gtcount) << " gavg2 " << gavg/1000
		<< " stdev " << stdev/1000
		<< "\n Average write segment length " << avgseg/2 << "KB" << endl;
	cout << "\nAverage write throughput : " 
		<< (sumseg * 512 * 1000000) / (1024 * 1024) / gwsum 
		<< "MB / Sec (Without flushing time)\n";
	cout << (sumseg * 512 * 1000000) / (1024 * 1024) / (gwsum + w_flush)
		<< "MB / Sec (With flushing time)\n\n";
}
*/
/*
void display_io_response(void) {
	unsigned int i, j;
	unsigned long int count, tcount, gtcount, tmin, tmax;
	double wsum, gwsum, avg, gavg, var, stdev;
	double avgseg, sumseg;

	cout << "Total read requests : " << READ_REQ_COUNT << endl
		<< "Total write requests : " << WRITE_REQ_COUNT << endl;
	cout << "Read \tblkcount \tavg \tstdev \tmin \tmax \treqs\n";
	gtcount = 0;
	gwsum = 0;
	sumseg = 0;
	for (i=0; i<1028; i++) {
		wsum = 0.0;
		count = tcount = 0;
		tmin = (DISK_READ_REQ_COUNT[i][0] > 0) ? 0 : 10000;
		tmax = 0;

		// Exclude zero units of time, which are served through
		// cached copies.
		for (j=0; j<2000; j++) {
			tcount += DISK_READ_REQ_COUNT[i][j];
			wsum += (DISK_READ_REQ_COUNT[i][j] * j);
			if (DISK_READ_REQ_COUNT[i][j] > 0) {
				if (tmin > j) tmin = j;
				if (tmax < j) tmax = j;
			}
			sumseg += (DISK_READ_REQ_COUNT[i][j] * (i+1));
		}

		if (tcount > 0) {
			avg = wsum / tcount;
			var = 0.0;
			for (j=0; j<2000; j++) {
				count = DISK_READ_REQ_COUNT[i][j];
				var += (((j - avg) * (j - avg)) * count);
			}
			stdev = sqrt(var / tcount);
			cout << "\t" << i+1 << "\t\t" << avg << "\t" << stdev << "\t" << tmin << "\t" << tmax << "\t" << tcount << endl;
		}
		gtcount += tcount;
		gwsum += wsum;
	}
	// Stdev
	gavg = gwsum / gtcount;
	avgseg = sumseg / gtcount;
	var = 0.0;

	for (i=0; i<1028; i++) {
		for (j=0; j<2000; j++) {
			count = DISK_READ_REQ_COUNT[i][j];
			var += (((j - gavg) * (j - gavg)) * count);
		}
	}

	stdev = sqrt(var / gtcount);

	cout << "Total disk read requests : " << gtcount << " gavg1 " 
		<< (DISK_READ_TOT_TIME[0] / gtcount) << " gavg2 " 
		<< gavg << " stdev " << stdev 
		<< "\n Average read segment length " << avgseg << endl;
	cout << "\nAverage read throughput : " 
		<< (sumseg * 512 / (1024 * 1024)) * 100 / gwsum 
		<< "MB / Sec\n\n";
	gtcount = 0;
	gwsum = 0;
	sumseg = 0;
	cout << "Write \tblkcount \tavg \tstdev \tmin \tmax \treqs\n";

	for (i=0; i<1028; i++) {
		wsum = 0.0;
		count = tcount = 0;
		tmin = (DISK_WRITE_REQ_COUNT[i][0] > 0) ? 0 : 10000;
		tmax = 0;

		for (j=0; j<2000; j++) {
			tcount += DISK_WRITE_REQ_COUNT[i][j];
			wsum += (DISK_WRITE_REQ_COUNT[i][j] * j);
			if (DISK_WRITE_REQ_COUNT[i][j] > 0) {
				if (tmin > j) tmin = j;
				if (tmax < j) tmax = j;
			}
			sumseg += (DISK_WRITE_REQ_COUNT[i][j] * (i+1));
		}

		if (tcount > 0) {
			avg = wsum / tcount;
			var = 0.0;
			for (j=0; j<2000; j++) {
				count = DISK_WRITE_REQ_COUNT[i][j];
				var += (((j - avg) * (j - avg)) * count);
			}
			stdev = sqrt(var / tcount);
			cout << "\t" << i+1 << "\t\t" << avg << "\t" << stdev << "\t" << tmin << "\t" << tmax << "\t" << tcount << endl;
		}
		gtcount += tcount;
		gwsum += wsum;
	}

	// Stdev
	gavg = gwsum / gtcount;
	avgseg = sumseg / gtcount;
	var = 0.0;

	for (i=0; i<1028; i++) {
		for (j=0; j<2000; j++) {
			count = DISK_WRITE_REQ_COUNT[i][j];
			var += (((j - gavg) * (j - gavg)) * count);
		}
	}

	stdev = sqrt(var / gtcount);

	cout << "Total disk write requests : " << gtcount << " gavg1 " 
		<< (DISK_WRITE_TOT_TIME[0] / gtcount) << " gavg2 " << gavg 
		<< " stdev " << stdev 
		<< "\n Average write segment length " << avgseg << endl;
	cout << "\nAverage write throughput : " 
		<< (sumseg * 512 / (1024 * 1024)) * 100 / gwsum 
		<< "MB / Sec\n\n";
}
*/
/*
unsigned long clock_start(void) {
	struct timespec t;

	CLOCK_MICRO = 0;
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	CLOCK_PREV = t.tv_sec * 1000000 + (t.tv_nsec / 1000);

	return CLOCK_MICRO;
}

unsigned long clock_suspend(void) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	CLOCK_MICRO += (t.tv_sec * 1000000 + (t.tv_nsec / 1000) - CLOCK_PREV);

	return CLOCK_MICRO;
}

unsigned long clock_restart(void) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	CLOCK_PREV = t.tv_sec * 1000000 + (t.tv_nsec / 1000);

	return CLOCK_MICRO;
}

unsigned long clock_stop(void) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	CLOCK_MICRO += (t.tv_sec * 1000000 + (t.tv_nsec / 1000) - CLOCK_PREV);

	return CLOCK_MICRO;
}
unsigned long clock_add(unsigned long v) {
	CLOCK_MICRO += v;
	return CLOCK_MICRO;
}


unsigned long disk_start(void) {
	DISK_MILLE = 0;
	DISK_PREV1 = DISK_READ_TOT_TIME[0];
	DISK_PREV2 = DISK_WRITE_TOT_TIME[0];

	return DISK_MILLE;
}

unsigned long disk_suspend(void) {
	unsigned long int t1, t2;
	t1 = DISK_READ_TOT_TIME[0];
	t2 = DISK_WRITE_TOT_TIME[0];

	DISK_MILLE += ((t1 - DISK_PREV1) + (t2 - DISK_PREV2));

	return DISK_MILLE;
}

unsigned long disk_restart(void) {
	DISK_PREV1 = DISK_READ_TOT_TIME[0];
	DISK_PREV2 = DISK_WRITE_TOT_TIME[0];

	return DISK_MILLE;
}

unsigned long disk_stop(void) {
	unsigned long int t1, t2;
	t1 = DISK_READ_TOT_TIME[0];
	t2 = DISK_WRITE_TOT_TIME[0];

	DISK_MILLE += ((t1 - DISK_PREV1) + (t2 - DISK_PREV2));

	return DISK_MILLE;
}
unsigned long disk_timeadd(unsigned long v) {
	DISK_MILLE += v;
	return DISK_MILLE;
}
*/

unsigned long clock_start(StorageNode *sn) {
	struct timespec t;

	sn->CLOCK_MICRO = 0;
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	sn->CLOCK_PREV = t.tv_sec * 1000000 + (t.tv_nsec / 1000);

	return sn->CLOCK_MICRO;
}

unsigned long clock_suspend(StorageNode *sn) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	sn->CLOCK_MICRO += (t.tv_sec * 1000000 + (t.tv_nsec / 1000) - sn->CLOCK_PREV);

	return sn->CLOCK_MICRO;
}

unsigned long clock_restart(StorageNode *sn) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	sn->CLOCK_PREV = t.tv_sec * 1000000 + (t.tv_nsec / 1000);

	return sn->CLOCK_MICRO;
}

unsigned long clock_stop(StorageNode *sn) {
	struct timespec t;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	//clock_gettime(CLOCK_REALTIME, &t);
	sn->CLOCK_MICRO += (t.tv_sec * 1000000 + (t.tv_nsec / 1000) - sn->CLOCK_PREV);

	return sn->CLOCK_MICRO;
}
unsigned long clock_add(StorageNode *sn, unsigned long v) {
	sn->CLOCK_MICRO += v;
	return sn->CLOCK_MICRO;
}

unsigned long clock_get(void) {
	struct timespec t;
	unsigned long v;

	//clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	clock_gettime(CLOCK_REALTIME, &t);
	v = (t.tv_sec * 1000000 + (t.tv_nsec / 1000));

	return v;
}

unsigned long clock_get(int clk) {
	struct timespec t;
	unsigned long v;

	//clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
	clock_gettime(clk, &t);
	v = (t.tv_sec * 1000000 + (t.tv_nsec / 1000));

	return v;
}

unsigned long disk_start(StorageNode *sn) {
	sn->DISK_MILLE = 0;
	sn->DISK_PREV1 = sn->DISK_READ_TOT_TIME[0];
	sn->DISK_PREV2 = sn->DISK_WRITE_TOT_TIME[0];

	return sn->DISK_MILLE;
}

unsigned long disk_suspend(StorageNode *sn) {
	unsigned long int t1, t2;
	t1 = sn->DISK_READ_TOT_TIME[0];
	t2 = sn->DISK_WRITE_TOT_TIME[0];

	sn->DISK_MILLE += ((t1 - sn->DISK_PREV1) + (t2 - sn->DISK_PREV2));

	return sn->DISK_MILLE;
}

unsigned long disk_restart(StorageNode *sn) {
	sn->DISK_PREV1 = sn->DISK_READ_TOT_TIME[0];
	sn->DISK_PREV2 = sn->DISK_WRITE_TOT_TIME[0];

	return sn->DISK_MILLE;
}

unsigned long disk_stop(StorageNode *sn) {
	unsigned long int t1, t2;
	t1 = sn->DISK_READ_TOT_TIME[0];
	t2 = sn->DISK_WRITE_TOT_TIME[0];

	sn->DISK_MILLE += ((t1 - sn->DISK_PREV1) + (t2 - sn->DISK_PREV2));

	return sn->DISK_MILLE;
}
unsigned long disk_timeadd(StorageNode *sn, unsigned long v) {
	sn->DISK_MILLE += v;
	return sn->DISK_MILLE;
}

void lockEntry(CacheEntry *c) {
	//c->ce_flags |= CACHE_ENTRY_BUSY;
//#ifdef	__DEBUG__
	if (c->ce_lckcnt == USHRT_MAX) {
		cout << "Key : " << c->ce_key << endl;
		assert(c->ce_lckcnt < USHRT_MAX);
	}
//#endif
	c->ce_lckcnt++;
}

void unlockEntry(CacheEntry *c) {
	//c->ce_flags &= (~CACHE_ENTRY_BUSY);
//#ifdef	__DEBUG__
	if (c->ce_lckcnt == 0) {
		cout << "Key : " << c->ce_key << endl;
		assert(c->ce_lckcnt > 0);
	}
//#endif
	c->ce_lckcnt--;
}

