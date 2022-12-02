#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <error.h>

#define DROP_CACHE_FILE "/proc/sys/vm/drop_caches"
// echo 3 > /proc/sys/vm/drop_caches
int main(int argc, char **argv) {

    int fd = open(DROP_CACHE_FILE, O_WRONLY);
    if (fd < 0) {
        char buf[255];
        snprintf(buf, 255, "Error while opening %s\n", DROP_CACHE_FILE);
        perror(buf);
        return -1;
    }
    
    char dropLevel[] = "3\n";
    ssize_t nBytesWritten = write(fd, dropLevel, 2);
    if (nBytesWritten != 2) {
        char buf[255];
        snprintf(buf, 255, "Error while writing to %s\n", DROP_CACHE_FILE);
        perror(buf);
        close(fd);
        return -2;
    }
    close(fd);
    return 0;
    
}

