/*
* engine.c - Supervised Multi-Container Runtime (User Space)
*/
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "monitor_ioctl.h"
#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
typedef enum {
CMD_SUPERVISOR = 0,
CMD_START,
CMD_RUN,
CMD_PS,
CMD_LOGS,
CMD_STOP
} command_kind_t;
typedef enum {
CONTAINER_STARTING = 0,
CONTAINER_RUNNING,
CONTAINER_STOPPED,
CONTAINER_KILLED,
CONTAINER_EXITED
} container_state_t;
typedef struct container_record {
char id[CONTAINER_ID_LEN];
pid_t host_pid;
time_t started_at;
container_state_t state;
unsigned long soft_limit_bytes;
unsigned long hard_limit_bytes;
int exit_code;
int exit_signal;
char log_path[PATH_MAX];
struct container_record *next;
} container_record_t;
typedef struct {
char container_id[CONTAINER_ID_LEN];
size_t length;
char data[LOG_CHUNK_SIZE];
} log_item_t;
typedef struct {
log_item_t items[LOG_BUFFER_CAPACITY];
size_t head;
size_t tail;
size_t count;
int shutting_down;
pthread_mutex_t mutex;
pthread_cond_t not_empty;
pthread_cond_t not_full;
} bounded_buffer_t;
typedef struct {
command_kind_t kind;
char container_id[CONTAINER_ID_LEN];
char rootfs[PATH_MAX];
char command[CHILD_COMMAND_LEN];
unsigned long soft_limit_bytes;
unsigned long hard_limit_bytes;
int nice_value;
} control_request_t;
typedef struct {
int status;
char message[CONTROL_MESSAGE_LEN];
} control_response_t;
typedef struct {
char id[CONTAINER_ID_LEN];
char rootfs[PATH_MAX];
char command[CHILD_COMMAND_LEN];
int nice_value;
int log_write_fd;
} child_config_t;
typedef struct {
int pipe_read_fd;
char container_id[CONTAINER_ID_LEN];
bounded_buffer_t *log_buffer;
} producer_args_t;
typedef struct {
int server_fd;
int monitor_fd;
int should_stop;
pthread_t logger_thread;
bounded_buffer_t log_buffer;
pthread_mutex_t metadata_lock;
container_record_t *containers;
} supervisor_ctx_t;
/* Global context pointer for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;
/* --------------------------------------------------------------------------
* Usage / argument parsing
* -------------------------------------------------------------------------- */
static void usage(const char *prog)
{
fprintf(stderr,
"Usage:\n"
"  %s supervisor <base-rootfs>\n"
"  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
"  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
"  %s ps\n"
"  %s logs <id>\n"
"  %s stop <id>\n",
prog, prog, prog, prog, prog, prog);
}
static int parse_mib_flag(const char *flag,
const char *value,
unsigned long *target_bytes)
{
char *end = NULL;
unsigned long mib;
errno = 0;
mib = strtoul(value, &end, 10);
if (errno != 0 || end == value || *end != '\0') {
fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
return -1;
}
if (mib > ULONG_MAX / (1UL << 20)) {
fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
return -1;
}
*target_bytes = mib * (1UL << 20);
return 0;
}
static int parse_optional_flags(control_request_t *req,
int argc,
char *argv[],
int start_index)
{
int i;
for (i = start_index; i < argc; i += 2) {
char *end = NULL;
long nice_value;
if (i + 1 >= argc) {
fprintf(stderr, "Missing value for option: %s\n", argv[i]);
return -1;
}
if (strcmp(argv[i], "--soft-mib") == 0) {
if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
return -1;
continue;
}
if (strcmp(argv[i], "--hard-mib") == 0) {
if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
return -1;
continue;
}
if (strcmp(argv[i], "--nice") == 0) {
errno = 0;
nice_value = strtol(argv[i + 1], &end, 10);
if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
nice_value < -20 || nice_value > 19) {
fprintf(stderr,
"Invalid value for --nice (expected -20..19): %s\n",
argv[i + 1]);
return -1;
}
req->nice_value = (int)nice_value;
continue;
}
fprintf(stderr, "Unknown option: %s\n", argv[i]);
return -1;
}
if (req->soft_limit_bytes > req->hard_limit_bytes) {
fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
return -1;
}
return 0;
}
static const char *state_to_string(container_state_t state)
{
switch (state) {
case CONTAINER_STARTING: return "starting";
case CONTAINER_RUNNING:  return "running";
case CONTAINER_STOPPED:  return "stopped";
case CONTAINER_KILLED:   return "killed";
case CONTAINER_EXITED:   return "exited";
default:                 return "unknown";
}
}
/* --------------------------------------------------------------------------
* Bounded buffer
* -------------------------------------------------------------------------- */
static int bounded_buffer_init(bounded_buffer_t *buffer)
{
int rc;
memset(buffer, 0, sizeof(*buffer));
rc = pthread_mutex_init(&buffer->mutex, NULL);
if (rc != 0)
return rc;
rc = pthread_cond_init(&buffer->not_empty, NULL);
if (rc != 0) {
pthread_mutex_destroy(&buffer->mutex);
return rc;
}
rc = pthread_cond_init(&buffer->not_full, NULL);
if (rc != 0) {
pthread_cond_destroy(&buffer->not_empty);
pthread_mutex_destroy(&buffer->mutex);
return rc;
}
return 0;
}
static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
pthread_cond_destroy(&buffer->not_full);
pthread_cond_destroy(&buffer->not_empty);
pthread_mutex_destroy(&buffer->mutex);
}
static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
pthread_mutex_lock(&buffer->mutex);
buffer->shutting_down = 1;
pthread_cond_broadcast(&buffer->not_empty);
pthread_cond_broadcast(&buffer->not_full);
pthread_mutex_unlock(&buffer->mutex);
}
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
pthread_mutex_lock(&buffer->mutex);
/* Wait while buffer is full */
while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
pthread_cond_wait(&buffer->not_full, &buffer->mutex);
/* If shutting down, don't insert */
if (buffer->shutting_down) {
pthread_mutex_unlock(&buffer->mutex);
return -1;
}
/* Insert item at tail */
buffer->items[buffer->tail] = *item;
buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
buffer->count++;
/* Wake up a consumer */
pthread_cond_signal(&buffer->not_empty);
pthread_mutex_unlock(&buffer->mutex);
return 0;
}
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
pthread_mutex_lock(&buffer->mutex);
/* Wait while buffer is empty */
while (buffer->count == 0 && !buffer->shutting_down)
pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
/* If shutting down and nothing left, signal done */
if (buffer->count == 0 && buffer->shutting_down) {
pthread_mutex_unlock(&buffer->mutex);
return -1;
}
/* Remove item from head */
*item = buffer->items[buffer->head];
buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
buffer->count--;
/* Wake up a producer */
pthread_cond_signal(&buffer->not_full);
pthread_mutex_unlock(&buffer->mutex);
return 0;
}
/* --------------------------------------------------------------------------
* Producer thread — reads container pipe, pushes into bounded buffer
* -------------------------------------------------------------------------- */
void *producer_thread(void *arg)
{
producer_args_t *args = (producer_args_t *)arg;
log_item_t item;
ssize_t n;
while (1) {
/* Read a chunk from the container pipe */
n = read(args->pipe_read_fd, item.data, LOG_CHUNK_SIZE);
if (n <= 0)
break; /* container exited or pipe closed */
/* Fill in the log item */
item.length = (size_t)n;
strncpy(item.container_id, args->container_id,
CONTAINER_ID_LEN - 1);
item.container_id[CONTAINER_ID_LEN - 1] = '\0';
/* Push into bounded buffer — consumer will write to file */
if (bounded_buffer_push(args->log_buffer, &item) != 0)
break; /* shutting down */
}
close(args->pipe_read_fd);
free(args);
return NULL;
}
/* --------------------------------------------------------------------------
* Logging thread (consumer)
* -------------------------------------------------------------------------- */
void *logging_thread(void *arg)
{
supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
log_item_t item;
char log_path[PATH_MAX];
int fd;
while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
/* Build log file path for this container */
snprintf(log_path, sizeof(log_path), "%s/%s.log",
LOG_DIR, item.container_id);
/* Open log file in append mode */
fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
if (fd < 0) {
perror("open log file");
continue;
}
write(fd, item.data, item.length);
close(fd);
}
return NULL;
}
/* --------------------------------------------------------------------------
* Container child entrypoint (runs inside cloned process)
* -------------------------------------------------------------------------- */
int child_fn(void *arg)
{
child_config_t *cfg = (child_config_t *)arg;
/* 1. Redirect stdout and stderr to the logging pipe */
dup2(cfg->log_write_fd, STDOUT_FILENO);
dup2(cfg->log_write_fd, STDERR_FILENO);
close(cfg->log_write_fd);
/* 2. Set hostname to container ID */
if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
perror("sethostname");
return 1;
}
/* 3. chroot into the container's rootfs */
if (chroot(cfg->rootfs) != 0) {
perror("chroot");
return 1;
}
/* 4. Change directory to / inside the container */
if (chdir("/") != 0) {
perror("chdir");
return 1;
}
/* 5. Mount /proc so ps, top etc. work inside container */
if (mount("proc", "/proc", "proc",
MS_NOEXEC | MS_NOSUID | MS_NODEV, NULL) != 0) {
perror("mount /proc");
return 1;
}
/* 6. Apply nice value if set */
if (cfg->nice_value != 0) {
if (nice(cfg->nice_value) == -1)
perror("nice");
}
/* 7. Execute the command inside the container */
char *argv_exec[] = { cfg->command, NULL };
execv(cfg->command, argv_exec);
/* If execv returns, something went wrong */
perror("execv");
return 1;
}
/* --------------------------------------------------------------------------
* Kernel monitor registration helpers
* -------------------------------------------------------------------------- */
int register_with_monitor(int monitor_fd,
const char *container_id,
pid_t host_pid,
unsigned long soft_limit_bytes,
unsigned long hard_limit_bytes)
{
struct monitor_request req;
memset(&req, 0, sizeof(req));
req.pid = host_pid;
req.soft_limit_bytes = soft_limit_bytes;
req.hard_limit_bytes = hard_limit_bytes;
strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
return -1;
return 0;
}
int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
struct monitor_request req;
memset(&req, 0, sizeof(req));
req.pid = host_pid;
strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
return -1;
return 0;
}
/* --------------------------------------------------------------------------
* Signal handlers
* -------------------------------------------------------------------------- */
static void handle_sigchld(int sig)
{
(void)sig;
int status;
pid_t pid;
/* Reap all exited children */
while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
if (!g_ctx) continue;
pthread_mutex_lock(&g_ctx->metadata_lock);
container_record_t *c = g_ctx->containers;
while (c) {
if (c->host_pid == pid) {
if (WIFEXITED(status)) {
c->state = CONTAINER_EXITED;
c->exit_code = WEXITSTATUS(status);
} else if (WIFSIGNALED(status)) {
c->exit_signal = WTERMSIG(status);
if (c->exit_signal == SIGKILL)
c->state = CONTAINER_KILLED;
else
c->state = CONTAINER_STOPPED;
}
break;
}
c = c->next;
}
pthread_mutex_unlock(&g_ctx->metadata_lock);
}
}
static void handle_shutdown(int sig)
{
(void)sig;
if (g_ctx)
g_ctx->should_stop = 1;
}
/* --------------------------------------------------------------------------
* Launch a container using clone()
* -------------------------------------------------------------------------- */
static pid_t launch_container(supervisor_ctx_t *ctx,
const control_request_t *req,
int log_write_fd)
{
char *stack;
char *stack_top;
pid_t pid;
child_config_t *cfg;
(void)ctx;
cfg = malloc(sizeof(child_config_t));
if (!cfg) {
perror("malloc cfg");
return -1;
}
strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
cfg->nice_value = req->nice_value;
cfg->log_write_fd = log_write_fd;
/* Allocate stack for the cloned child */
stack = malloc(STACK_SIZE);
if (!stack) {
perror("malloc stack");
free(cfg);
return -1;
}
stack_top = stack + STACK_SIZE; /* stack grows downward */
/* Clone with new PID, UTS, and mount namespaces */
pid = clone(child_fn,
stack_top,
CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
cfg);
if (pid < 0) {
perror("clone");
free(stack);
free(cfg);
return -1;
}
free(stack); /* stack is copied by kernel, safe to free */
return pid;
}
/* --------------------------------------------------------------------------
* Supervisor command handlers
* -------------------------------------------------------------------------- */
static void handle_ps_command(supervisor_ctx_t *ctx, control_response_t *resp)
{
char line[256];
int offset = 0;
memset(resp, 0, sizeof(*resp));
resp->status = 0;
pthread_mutex_lock(&ctx->metadata_lock);
container_record_t *c = ctx->containers;
if (!c) {
snprintf(resp->message, sizeof(resp->message), "No containers running.");
pthread_mutex_unlock(&ctx->metadata_lock);
return;
}
while (c && offset < (int)sizeof(resp->message) - 1) {
snprintf(line, sizeof(line),
"[%s] pid=%d state=%s\n",
c->id, c->host_pid, state_to_string(c->state));
int len = strlen(line);
if (offset + len < (int)sizeof(resp->message) - 1) {
memcpy(resp->message + offset, line, len);
offset += len;
}
c = c->next;
}
resp->message[offset] = '\0';
pthread_mutex_unlock(&ctx->metadata_lock);
}
static void handle_logs_command(supervisor_ctx_t *ctx,
const control_request_t *req,
int client_fd)
{
char log_path[PATH_MAX];
char buf[1024];
int fd;
ssize_t n;
control_response_t resp;
(void)ctx;
snprintf(log_path, sizeof(log_path), "%s/%s.log",
LOG_DIR, req->container_id);
fd = open(log_path, O_RDONLY);
if (fd < 0) {
memset(&resp, 0, sizeof(resp));
resp.status = -1;
snprintf(resp.message, sizeof(resp.message),
"No log found for container: %s", req->container_id);
write(client_fd, &resp, sizeof(resp));
return;
}
/* Send OK response first */
memset(&resp, 0, sizeof(resp));
resp.status = 0;
snprintf(resp.message, sizeof(resp.message), "Log for %s:", req->container_id);
write(client_fd, &resp, sizeof(resp));
/* Stream the log file to client */
while ((n = read(fd, buf, sizeof(buf))) > 0)
write(client_fd, buf, n);
close(fd);
}
static void handle_stop_command(supervisor_ctx_t *ctx,
const control_request_t *req,
control_response_t *resp)
{
memset(resp, 0, sizeof(*resp));
pthread_mutex_lock(&ctx->metadata_lock);
container_record_t *c = ctx->containers;
while (c) {
if (strcmp(c->id, req->container_id) == 0) {
if (c->state == CONTAINER_RUNNING) {
kill(c->host_pid, SIGTERM);
c->state = CONTAINER_STOPPED;
resp->status = 0;
snprintf(resp->message, sizeof(resp->message),
"Sent SIGTERM to container %s (pid %d)",
c->id, c->host_pid);
} else {
resp->status = -1;
snprintf(resp->message, sizeof(resp->message),
"Container %s is not running", c->id);
}
pthread_mutex_unlock(&ctx->metadata_lock);
return;
}
c = c->next;
}
pthread_mutex_unlock(&ctx->metadata_lock);
resp->status = -1;
snprintf(resp->message, sizeof(resp->message),
"Container not found: %s", req->container_id);
}
static void handle_start_command(supervisor_ctx_t *ctx,
const control_request_t *req,
control_response_t *resp)
{
int pipe_fds[2];
pid_t pid;
container_record_t *record;
memset(resp, 0, sizeof(*resp));
/* Create pipe: container writes, supervisor reads */
if (pipe(pipe_fds) < 0) {
perror("pipe");
resp->status = -1;
snprintf(resp->message, sizeof(resp->message), "pipe() failed");
return;
}
/* Launch the container */
pid = launch_container(ctx, req, pipe_fds[1]);
close(pipe_fds[1]); /* parent doesn't write */
if (pid < 0) {
close(pipe_fds[0]);
resp->status = -1;
snprintf(resp->message, sizeof(resp->message),
"Failed to launch container %s", req->container_id);
return;
}
/* Register with kernel monitor */
if (ctx->monitor_fd >= 0)
register_with_monitor(ctx->monitor_fd, req->container_id,
pid, req->soft_limit_bytes,
req->hard_limit_bytes);
/* Add metadata record */
record = calloc(1, sizeof(container_record_t));
strncpy(record->id, req->container_id, sizeof(record->id) - 1);
record->host_pid = pid;
record->started_at = time(NULL);
record->state = CONTAINER_RUNNING;
record->soft_limit_bytes = req->soft_limit_bytes;
record->hard_limit_bytes = req->hard_limit_bytes;
snprintf(record->log_path, sizeof(record->log_path),
"%s/%s.log", LOG_DIR, req->container_id);
pthread_mutex_lock(&ctx->metadata_lock);
record->next = ctx->containers;
ctx->containers = record;
pthread_mutex_unlock(&ctx->metadata_lock);
/* Spawn producer thread to read container output into log buffer */
producer_args_t *pargs = malloc(sizeof(producer_args_t));
if (pargs) {
pargs->pipe_read_fd = pipe_fds[0];
pargs->log_buffer = &ctx->log_buffer;
strncpy(pargs->container_id, req->container_id,
CONTAINER_ID_LEN - 1);
pargs->container_id[CONTAINER_ID_LEN - 1] = '\0';
pthread_t prod_thread;
if (pthread_create(&prod_thread, NULL, producer_thread, pargs) != 0) {
perror("pthread_create producer");
close(pipe_fds[0]);
free(pargs);
} else {
pthread_detach(prod_thread); /* auto-cleanup when done */
}
} else {
close(pipe_fds[0]);
}
resp->status = 0;
snprintf(resp->message, sizeof(resp->message),
"Started container %s with pid %d", req->container_id, pid);
}
/* --------------------------------------------------------------------------
* Long-running supervisor process
* -------------------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
supervisor_ctx_t ctx;
struct sockaddr_un addr;
struct sigaction sa;
int rc;
(void)rootfs;
memset(&ctx, 0, sizeof(ctx));
ctx.server_fd = -1;
ctx.monitor_fd = -1;
g_ctx = &ctx;
/* Create logs directory */
mkdir(LOG_DIR, 0755);
/* Init metadata lock */
rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
if (rc != 0) {
errno = rc;
perror("pthread_mutex_init");
return 1;
}
/* Init bounded buffer */
rc = bounded_buffer_init(&ctx.log_buffer);
if (rc != 0) {
errno = rc;
perror("bounded_buffer_init");
pthread_mutex_destroy(&ctx.metadata_lock);
return 1;
}
/* 1. Open kernel monitor device */
ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
if (ctx.monitor_fd < 0)
fprintf(stderr, "Warning: could not open monitor device: %s\n",
strerror(errno));
/* 2. Create UNIX domain control socket */
ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
if (ctx.server_fd < 0) {
perror("socket");
return 1;
}
unlink(CONTROL_PATH); /* remove stale socket if any */
memset(&addr, 0, sizeof(addr));
addr.sun_family = AF_UNIX;
strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
perror("bind");
return 1;
}
if (listen(ctx.server_fd, 8) < 0) {
perror("listen");
return 1;
}
/* 3. Install signal handlers */
memset(&sa, 0, sizeof(sa));
sa.sa_handler = handle_sigchld;
sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
sigaction(SIGCHLD, &sa, NULL);
sa.sa_handler = handle_shutdown;
sa.sa_flags = 0;
sigaction(SIGINT, &sa, NULL);
sigaction(SIGTERM, &sa, NULL);
/* 4. Start logger thread */
rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
if (rc != 0) {
errno = rc;
perror("pthread_create");
return 1;
}
fprintf(stdout, "Supervisor ready. Listening on %s\n", CONTROL_PATH);
fflush(stdout);
/* 5. Event loop: accept and handle CLI commands */
while (!ctx.should_stop) {
int client_fd;
control_request_t req;
control_response_t resp;
client_fd = accept(ctx.server_fd, NULL, NULL);
if (client_fd < 0) {
if (errno == EINTR) continue; /* interrupted by signal, check should_stop */
perror("accept");
break;
}
if (read(client_fd, &req, sizeof(req)) != sizeof(req)) {
close(client_fd);
continue;
}
switch (req.kind) {
case CMD_START:
case CMD_RUN:
handle_start_command(&ctx, &req, &resp);
write(client_fd, &resp, sizeof(resp));
break;
case CMD_PS:
handle_ps_command(&ctx, &resp);
write(client_fd, &resp, sizeof(resp));
break;
case CMD_LOGS:
handle_logs_command(&ctx, &req, client_fd);
break;
case CMD_STOP:
handle_stop_command(&ctx, &req, &resp);
write(client_fd, &resp, sizeof(resp));
break;
default:
memset(&resp, 0, sizeof(resp));
resp.status = -1;
snprintf(resp.message, sizeof(resp.message), "Unknown command");
write(client_fd, &resp, sizeof(resp));
}
close(client_fd);
}
/* Shutdown sequence */
fprintf(stdout, "\nSupervisor shutting down...\n");
close(ctx.server_fd);
unlink(CONTROL_PATH);
bounded_buffer_begin_shutdown(&ctx.log_buffer);
pthread_join(ctx.logger_thread, NULL);
bounded_buffer_destroy(&ctx.log_buffer);
if (ctx.monitor_fd >= 0)
close(ctx.monitor_fd);
/* Free container metadata list */
pthread_mutex_lock(&ctx.metadata_lock);
container_record_t *c = ctx.containers;
while (c) {
container_record_t *next = c->next;
free(c);
c = next;
}
pthread_mutex_unlock(&ctx.metadata_lock);
pthread_mutex_destroy(&ctx.metadata_lock);
fprintf(stdout, "Supervisor exited cleanly.\n");
return 0;
}
/* --------------------------------------------------------------------------
* CLI client: send a control request to the supervisor
* -------------------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
int sock_fd;
struct sockaddr_un addr;
control_response_t resp;
sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
if (sock_fd < 0) {
perror("socket");
return 1;
}
memset(&addr, 0, sizeof(addr));
addr.sun_family = AF_UNIX;
strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
perror("connect: is the supervisor running?");
close(sock_fd);
return 1;
}
if (write(sock_fd, req, sizeof(*req)) != sizeof(*req)) {
perror("write request");
close(sock_fd);
return 1;
}
if (read(sock_fd, &resp, sizeof(resp)) != sizeof(resp)) {
perror("read response");
close(sock_fd);
return 1;
}
printf("%s\n", resp.message);
close(sock_fd);
return resp.status == 0 ? 0 : 1;
}
/* --------------------------------------------------------------------------
* CLI command entry points
* -------------------------------------------------------------------------- */
static int cmd_start(int argc, char *argv[])
{
control_request_t req;
if (argc < 5) {
fprintf(stderr,
"Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
argv[0]);
return 1;
}
memset(&req, 0, sizeof(req));
req.kind = CMD_START;
strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
strncpy(req.command, argv[4], sizeof(req.command) - 1);
req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
if (parse_optional_flags(&req, argc, argv, 5) != 0)
return 1;
return send_control_request(&req);
}
static int cmd_run(int argc, char *argv[])
{
control_request_t req;
if (argc < 5) {
fprintf(stderr,
"Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
argv[0]);
return 1;
}
memset(&req, 0, sizeof(req));
req.kind = CMD_RUN;
strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
strncpy(req.command, argv[4], sizeof(req.command) - 1);
req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
if (parse_optional_flags(&req, argc, argv, 5) != 0)
return 1;
return send_control_request(&req);
}
static int cmd_ps(void)
{
control_request_t req;
memset(&req, 0, sizeof(req));
req.kind = CMD_PS;
return send_control_request(&req);
}
static int cmd_logs(int argc, char *argv[])
{
int sock_fd;
struct sockaddr_un addr;
control_request_t req;
control_response_t resp;
char buf[1024];
ssize_t n;
if (argc < 3) {
fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
return 1;
}
memset(&req, 0, sizeof(req));
req.kind = CMD_LOGS;
strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
if (sock_fd < 0) { perror("socket"); return 1; }
memset(&addr, 0, sizeof(addr));
addr.sun_family = AF_UNIX;
strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
perror("connect");
close(sock_fd);
return 1;
}
if (write(sock_fd, &req, sizeof(req)) != sizeof(req)) {
perror("write");
close(sock_fd);
return 1;
}
/* Read the response header */
if (read(sock_fd, &resp, sizeof(resp)) != sizeof(resp)) {
perror("read");
close(sock_fd);
return 1;
}
printf("%s\n", resp.message);
if (resp.status == 0) {
/* Stream remaining log content to stdout */
while ((n = read(sock_fd, buf, sizeof(buf))) > 0)
fwrite(buf, 1, n, stdout);
}
close(sock_fd);
return resp.status == 0 ? 0 : 1;
}
static int cmd_stop(int argc, char *argv[])
{
control_request_t req;
if (argc < 3) {
fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
return 1;
}
memset(&req, 0, sizeof(req));
req.kind = CMD_STOP;
strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
return send_control_request(&req);
}
/* --------------------------------------------------------------------------
* main
* -------------------------------------------------------------------------- */
int main(int argc, char *argv[])
{
if (argc < 2) {
usage(argv[0]);
return 1;
}
if (strcmp(argv[1], "supervisor") == 0) {
if (argc < 3) {
fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
return 1;
}
return run_supervisor(argv[2]);
}
if (strcmp(argv[1], "start") == 0)
return cmd_start(argc, argv);
if (strcmp(argv[1], "run") == 0)
return cmd_run(argc, argv);
if (strcmp(argv[1], "ps") == 0)
return cmd_ps();
if (strcmp(argv[1], "logs") == 0)
return cmd_logs(argc, argv);
if (strcmp(argv[1], "stop") == 0)
return cmd_stop(argc, argv);
usage(argv[0]);
return 1;
}

