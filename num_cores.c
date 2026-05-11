#include <stdio.h>
#include <unistd.h>

// counts the number of logical cores on the machine and prints it out
int main() {
    int cores = sysconf(_SC_NPROCESSORS_ONLN);
    printf("This machine has %d cores.\n", cores);
    return 0;
}
