CC=gcc
CFLAGS=-Wall -Wextra

all: engine cpu_hog memory_hog io_pulse

engine: engine.c
	$(CC) $(CFLAGS) engine.c -o engine

cpu_hog: cpu_hog.c
	$(CC) $(CFLAGS) cpu_hog.c -o cpu_hog

memory_hog: memory_hog.c
	$(CC) $(CFLAGS) memory_hog.c -o memory_hog

io_pulse: io_pulse.c
	$(CC) $(CFLAGS) io_pulse.c -o io_pulse

clean:
	rm -f engine cpu_hog memory_hog io_pulse *.o *.ko *.mod.c Module.symvers modules.order

# Kernel module
obj-m += monitor.o

KDIR := /lib/modules/$(shell uname -r)/build
PWD  := $(shell pwd)

monitor:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean-monitor:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
