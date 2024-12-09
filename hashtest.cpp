#include <iostream>
#include <sys/time.h>

using namespace std;

void md5(unsigned char *buf, unsigned int nbytes, unsigned char hash[16]);

unsigned char buf[4096], myhash[16];

int main(int argc, char *argv[]) {
	int i, n;
	unsigned long long int t1, t2;
	struct timespec tval;

	n = atoi(argv[1]);

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t1 = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	for (i=0; i<n; i++) 
		md5(buf, 512, myhash);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2 = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	cout << " Time(512) " << (t2 - t1) << endl;


	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t1 = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	for (i=0; i<n; i++) 
		md5(buf, 4096, myhash);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2 = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	cout << " Time(4096) " << (t2 - t1) << endl;

	return 0;
}
