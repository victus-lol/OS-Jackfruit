#  OS-Jackfruit: Container Runtime & Memory Monitor

##  Overview

OS-Jackfruit is a lightweight container runtime and monitoring system built using **user-space scheduling** and a **Linux Kernel Module (LKM)**. It demonstrates core Operating Systems concepts such as:

* Process management and scheduling
* Containerization using namespaces (basic runtime model)
* Kernel ↔ user-space communication via IOCTL
* Memory monitoring using a kernel module

---

##  Project Structure

```
OS-Jackfruit/
│
├── boilerplate/
│   ├── engine.c              # User-space container runtime & CLI
│   ├── monitor.c             # Kernel module for memory monitoring
│   ├── monitor_ioctl.h       # Shared IOCTL definitions
│   ├── cpu_hog.c             # CPU stress workload
│   ├── memory_hog.c          # Memory stress workload
│   ├── io_pulse.c            # I/O workload
│   ├── Makefile              # Build system
│   └── environment-check.sh  # Environment validation script
│
├── README.md
├── project-guide.md
```

---

##  Features

###  Container Runtime (`engine`)

* CLI-based container manager
* Supports:

  * `supervisor` mode (daemon)
  * `run`, `start`, `stop`, `ps`, `logs`
* Uses process isolation concepts
* Can enforce resource limits (extendable)

---

###  Kernel Module (`monitor`)

* Linux Kernel Module for memory monitoring
* Communicates with user-space using **IOCTL**
* Tracks memory usage of processes/containers

---

###  Workloads

Used for testing scheduling and monitoring:

| Program      | Purpose           |
| ------------ | ----------------- |
| `cpu_hog`    | High CPU usage    |
| `memory_hog` | High memory usage |
| `io_pulse`   | Simulated I/O     |

---

##  Build Instructions

### 1️ Navigate to project

```bash
cd OS-Jackfruit/boilerplate
```

### 2️ Build user-space programs

```bash
make clean
make
```

### 3️ Build kernel module

```bash
make monitor
```

---

##  Running the Project

###  Step 1: Load kernel module

```bash
sudo insmod monitor.ko
```

Check:

```bash
lsmod | grep monitor
```

---

###  Step 2: Prepare minimal root filesystem

```bash
mkdir -p /tmp/jackfruit-rootfs/bin
cp /bin/sh /tmp/jackfruit-rootfs/bin/
```

---

###  Step 3: Start supervisor (daemon)

```bash
sudo ./engine supervisor /tmp/jackfruit-rootfs
```

---

###  Step 4: Run a container (in another terminal)

```bash
sudo ./engine run c1 /tmp/jackfruit-rootfs /bin/sh
```

---

###  Step 5: Manage containers

```bash
sudo ./engine ps
sudo ./engine logs c1
sudo ./engine stop c1
```

---

##  Running Workloads

Example:

```bash
sudo ./engine run test1 /tmp/jackfruit-rootfs ./cpu_hog
```

---

##  Cleanup

### Stop containers

```bash
sudo ./engine stop <id>
```

### Remove kernel module

```bash
sudo rmmod monitor
```

---

##  Common Issues

###  Permission denied

 Run all engine commands with `sudo`

---

###  Module already exists

```bash
sudo rmmod monitor
sudo insmod monitor.ko
```

---

###  Network / Git issues

Ensure VM has internet and DNS configured correctly

---

##  Concepts Demonstrated

* Process creation (`fork`, `exec`)
* Scheduling (Round Robin / control via signals)
* Inter-process communication
* Kernel module development
* IOCTL interface design
* Basic container runtime architecture

---

##  Learning Outcomes

By completing this project, we understand:

* How user-space interacts with the kernel
* How containers are managed internally
* How system resources can be monitored and controlled

---

##  License

This project is for academic use.
