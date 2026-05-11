#include <stdio.h>
#include <unistd.h>
 
int main() {
    int cores = sysconf(_SC_NPROCESSORS_ONLN);
    printf("This machine has %d cores.\n", cores);
    return 0;
}
