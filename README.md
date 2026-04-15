# Mini Container Runtime with Kernel Memory Monitoring

##  Overview

This project implements a lightweight container runtime in Linux using low-level system calls and a kernel module. It demonstrates key operating system concepts such as process isolation, inter-process communication, scheduling, and kernel-level resource monitoring.

The system allows multiple containers to run concurrently under a supervisor while enforcing memory limits using a Loadable Kernel Module (LKM).

---

##  Project Components

### 1. User-Space Runtime

* **engine.c**

  * Implements container creation using `clone()`
  * Supports execution of commands inside containers
  * Provides CLI interface (`start`, `ps`, `logs`, `stop`)
  * Communicates with kernel module using `ioctl`

### 2. Kernel-Space Monitor

* **monitor.c**

  * Loadable Kernel Module (LKM)
  * Tracks memory usage of containers
  * Enforces:

    * Soft memory limits (warning)
    * Hard memory limits (process termination)

### 3. Shared Interface

* **monitor_ioctl.h**

  * Defines IOCTL commands and data structures
  * Enables communication between user-space and kernel-space

### 4. Workload Programs

* **mem_test.c**

  * Continuously allocates memory
  * Used to trigger soft and hard memory limits

* **cpu_test.c**

  * CPU-intensive loop
  * Used for scheduling experiments

### 5. Build System

* **Makefile**

  * Compiles both user-space and kernel-space components using a single command

---

##  Features

* Lightweight container creation using Linux namespaces
* Multiple container execution
* Supervisor-based management
* Kernel-level memory monitoring
* Soft and hard memory enforcement
* IOCTL-based communication
* Logging support
* Scheduling experiments using `nice`

---

##  Build Instructions

```bash
make clean
make
```

---

##  Execution Steps

### 1. Load Kernel Module

```bash
sudo insmod monitor.ko
```

### 2. Create Device File (if not already present)

```bash
grep container_monitor /proc/devices
sudo mknod /dev/container_monitor c <major_number> 0
sudo chmod 666 /dev/container_monitor
```

### 3. Start a Container

```bash
sudo ./engine start c1 /bin/bash
```

### 4. Run Multiple Containers

```bash
sudo ./engine start c2 /bin/ls
sudo ./engine start c3 /bin/date
```

### 5. View Kernel Logs

```bash
dmesg | tail
```

### 6. Stop Container

```bash
kill <pid>
```

---

## Testing

### Memory Test

```bash
./mem_test
```

* Triggers soft and hard memory limits
* Observe using:

```bash
dmesg
```

### Scheduling Test

```bash
nice -n 10 ./cpu_test
nice -n -5 ./cpu_test
```

* Compare CPU scheduling behavior

---

##  Expected Output

* Container creation messages
* Kernel logs showing:

  * Container registration
  * Soft limit warnings
  * Hard limit enforcement
* Process execution inside containers
* Scheduling differences using `nice`

---

##  Concepts Demonstrated

* Process isolation using `clone()`
* PID namespaces
* Kernel module programming
* IOCTL communication
* Memory management in OS
* CPU scheduling
* Inter-process communication

---

## Project Structure

```
engine.c
monitor.c
monitor_ioctl.h
Makefile
README.md
mem_test.c
cpu_test.c
logs/
monitor.ko
engine
```

---

##  Notes

* Kernel module may show a "signature verification failed" warning — this is normal in virtual machines.
* Root privileges (`sudo`) are required for module loading and container execution.

---

## Conclusion

This project successfully demonstrates the design and implementation of a minimal container runtime with kernel-level monitoring. It highlights practical applications of operating system concepts such as isolation, resource control, and scheduling.

---
