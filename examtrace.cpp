/**********************************************************************
 *  This program is to give the statistics about the trace input      *
 *								      *
 **********************************************************************/
#include <iostream>
#include <fstream>
#include <assert.h>

#include "iotracereader.h"

char fnames[][100] = {
/*
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.1.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.2.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.3.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.4.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.5.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.6.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.7.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.8.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.9.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.10.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.11.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.12.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.13.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.14.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.15.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.16.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.17.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.18.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.19.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.20.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.21.blkparse"
*/
/*
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.2.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.3.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.4.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.5.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.6.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.7.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.8.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.9.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.10.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.11.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.12.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.13.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.14.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.15.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.16.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.17.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.18.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.19.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.20.blkparse",
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.21.blkparse"
*/

/*
	"../../../../mail-01/sample-10000"

	"../input/sample-10000"
*/

/*
	"../input/cheetah.cs.fiu.edu-110108-113008.2.blkparse"
*/
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse"
};


unsigned int reccount100000 = 1;


/************************************************/
// Read and write segment length wise counts
unsigned long long int segments[2][512] = {{0}, {0}}; 
unsigned long long int readCount = 0, writeCount = 0;


/************************************************/

int main(int argc, char** argv) {
	IOTraceReader *trace;
#ifdef	HOME
	IORecordHome rec1, rec2;
#else
	IORecord4K rec1, rec2;
#endif
	int nb;
	int i; 
	int count = 0; 

#ifdef	HOME
	trace = new IOTraceReaderHome();
	if (trace->openTrace(fnames, 1, 512) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#else
	trace = new IOTraceReader4K();
	if (trace->openTrace(fnames, 1, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#endif


	trace->readNext(rec1);
	nb = rec1.ior_nb;
	count = 1;
	while (trace->readNext(rec2)) {
		// Part of the same segment?
		if ((nb < 512) && (rec1.ior_pid == rec2.ior_pid) &&
			(rec1.ior_lba + nb == rec2.ior_lba) &&
			(rec1.ior_rw == rec2.ior_rw)) {
			nb += rec2.ior_nb;
			count++;
		}
		else {
			// End of the segment, update statistics
			if (rec1.ior_rw == 'R') {
				segments[0][nb-1]++;
				readCount += count;
			}
			else {
				segments[1][nb-1]++;
				writeCount += count;
			}
			rec1 = rec2;
			nb = rec1.ior_nb;
			count = 1;
		}

		if (trace->getCurCount() >= (reccount100000 * 500000)) {
			cout << trace->getCurCount() << "  number of records processed\n";
			reccount100000++;
		}

	}

	cout << trace->getCurCount() << "  number of records processed\n";

	unsigned long count1, count2;
	count1 = count2 = 0;
	cout << "\t#Blocks\t\t#Reads\t\t#Writes\n";
	for (i=0; i<512; i++) {
		if ((segments[0][i] != 0) || 
			(segments[1][i] != 0)) {
			cout << "\t" << i+1 << "\t\t" << segments[0][i]
				<< "\t\t" << segments[1][i] << endl;
			count1 += segments[0][i];
			count2 += segments[1][i];
		}
	}
	cout << "Total\t\t\t" << count1 << "\t\t" << count2 << endl;
	cout << "\n\nTotal requests \tRead \t: " << readCount << "\tWrite \t: " << writeCount << endl;

	trace->closeTrace();	

	return 0;
}
