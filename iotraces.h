// All traces file names
#ifndef	__IOTRACES_H__
#define __IOTRACES_H__ 1

// Counts of trace file names, deduplication types and file names
int fcount[6] = {1, 1, 1, 12, 2, 2};
int ftype[6] = {UTYPE, UTYPE, UTYPE, HTYPE, HTYPE, LTYPE};
int fbsize[6] = {4096, 4096, 512, 4096, 4096, 4096};
char fnames[6][21][100] = {
{
"../../../../localtrace/mail-all-trace.txt"
},
{
"../../../../localtrace/web-all-trace.txt"
},
{
"../../../../localtrace/home-all-trace.txt"
},
/*
{
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
},
{
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
},
{
"../../../../Homes/homes-110108-112108.1.blkparse",
"../../../../Homes/homes-110108-112108.2.blkparse",
"../../../../Homes/homes-110108-112108.3.blkparse",
"../../../../Homes/homes-110108-112108.4.blkparse",
"../../../../Homes/homes-110108-112108.5.blkparse",
"../../../../Homes/homes-110108-112108.6.blkparse",
"../../../../Homes/homes-110108-112108.7.blkparse",
"../../../../Homes/homes-110108-112108.8.blkparse",
"../../../../Homes/homes-110108-112108.9.blkparse",
"../../../../Homes/homes-110108-112108.10.blkparse",
"../../../../Homes/homes-110108-112108.11.blkparse",
"../../../../Homes/homes-110108-112108.12.blkparse",
"../../../../Homes/homes-110108-112108.13.blkparse",
"../../../../Homes/homes-110108-112108.14.blkparse",
"../../../../Homes/homes-110108-112108.15.blkparse",
"../../../../Homes/homes-110108-112108.16.blkparse",
"../../../../Homes/homes-110108-112108.17.blkparse",
"../../../../Homes/homes-110108-112108.18.blkparse",
"../../../../Homes/homes-110108-112108.19.blkparse",
"../../../../Homes/homes-110108-112108.20.blkparse",
"../../../../Homes/homes-110108-112108.21.blkparse"
},
*/
{
"../../../../localtrace/linux-5.0-trace.txt",
"../../../../localtrace/linux-5.1-trace.txt",
"../../../../localtrace/linux-5.2-trace.txt",
"../../../../localtrace/linux-5.3-trace.txt",
"../../../../localtrace/linux-5.4-trace.txt",
"../../../../localtrace/linux-5.5-trace.txt",
"../../../../localtrace/linux-5.6-trace.txt",
"../../../../localtrace/linux-5.7-trace.txt",
"../../../../localtrace/linux-5.8-trace.txt",
"../../../../localtrace/linux-5.9-trace.txt",
"../../../../localtrace/linux-5.10-trace.txt",
"../../../../localtrace/linux-5.11-trace.txt"
},
{
"../../../../localtrace/books-trace.txt",
"../../../../localtrace/slides-trace.txt"
},
{
"../../../../localtrace/imgvid-trace.txt",
"../../../../localtrace/nptel-trace.txt"
}
};

char frnames[6][100] = {
"../../../../localtrace/mail-read-trace.txt",
"../../../../localtrace/web-read-trace.txt",
"../../../../localtrace/home-read-trace.txt",
"../../../../localtrace/linux-read-trace.txt",
"../../../../localtrace/bookppt-read-trace.txt",
"../../../../localtrace/vidimg-read-trace.txt"
};

/* Information about the traces
 *	(dev)	records		reads(blocks)		writes(blocks)
 Web - 	(2,3)	14294158	24931648		89421616
 Mail -	(1,0)	460334027	410786016		3271886200
 Home - (3,0)	17836701	32418889		136881792
 Linux- (7,2)	86418389	205236432
 imgvid-(9,0)	21838832	162206568
 nptel- (9,0)	33938769
 books- (9,0)	4835460		47644784
 slides-(9,0)	4909891
*/
#endif
