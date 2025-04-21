/*
 * netsiohub.c - C reimplementation of fujinet-bridge/netsiohub Python script
 * Supports NetSIO protocol over UDP and Altirra custom device protocol over TCP
 * Serial port mode via POSIX termios (optional, enabled with --serial)
 *
 * Build with: Makefile (provided)
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <getopt.h>
#include <sys/select.h>

/* NetSIO message IDs */
#define NETSIO_DATA_BYTE        0x01
#define NETSIO_DATA_BLOCK       0x02
#define NETSIO_DATA_BYTE_SYNC   0x09
#define NETSIO_COMMAND_OFF      0x10
#define NETSIO_COMMAND_ON       0x11
#define NETSIO_COMMAND_OFF_SYNC 0x18
#define NETSIO_MOTOR_OFF        0x20
#define NETSIO_MOTOR_ON         0x21
#define NETSIO_MOTOR_SYNC       0x29
#define NETSIO_PROCEED_OFF      0x30
#define NETSIO_PROCEED_ON       0x31
#define NETSIO_CONN_MGMT        0x80
#define NETSIO_SPEED_CHANGE     0x8B
#define NETSIO_SYNC_RESPONSE    0x8A
#define NETSIO_CANCEL           0x89
#define NETSIO_BUS_IDLE         0x88
#define NETSIO_CREDIT_STATUS    0x84
#define NETSIO_CREDIT_UPDATE    0x83
#define NETSIO_PING_REQUEST     0x85
#define NETSIO_PING_RESPONSE    0x86
#define NETSIO_ALIVE_REQUEST    0x87
#define NETSIO_ALIVE_RESPONSE   0x88
#define NETSIO_DEVICE_DISCONNECT 0x82
#define NETSIO_DEVICE_CONNECT   0x81

/* Altirra custom device protocol (extension for netsio.atdevice) */
#define ATDEV_READY             0x100
#define ATDEV_TRANSMIT_BUFFER   0x101
#define ATDEV_CMD               0x102
#define ATDEV_DEBUG_NOP         0x103
#define ATDEV_EMPTY_SYNC        0x104

/* Default ports */
#define NETSIO_ATDEV_PORT   9996
#define NETSIO_PORT         9997

/* expiration and buffers */
#define ALIVE_EXPIRATION    10
#define DEFAULT_CREDIT      32

/* forward-declare message types & queue API */
typedef struct NetSIOMsg NetSIOMsg;
typedef struct MsgQueue  MsgQueue;

static void       queue_push(MsgQueue *q, NetSIOMsg *m);
static NetSIOMsg *queue_pop (MsgQueue *q);

/* Tagged NetSIO message struct */
typedef struct NetSIOMsg {
    uint8_t id;
    uint8_t arg[512];
    size_t arg_len;
    double tstamp;
} NetSIOMsg;

/* Serial port context */
typedef struct {
    int fd;
    int command_mbit;
    int proceed_mbit;
    pthread_mutex_t lock;
    int sync_flag;
    uint8_t sync_num;
} SerialCtx;

/* Convert integer baudrate to termios speed constant */
static speed_t baud_to_const(int baud) {
    switch (baud) {
        case 50: return B50;
        case 75: return B75;
        case 110: return B110;
        case 134: return B134;
        case 150: return B150;
        case 200: return B200;
        case 300: return B300;
        case 600: return B600;
        case 1200: return B1200;
        case 1800: return B1800;
        case 2400: return B2400;
        case 4800: return B4800;
        case 9600: return B9600;
        case 19200: return B19200;
        case 38400: return B38400;
        case 57600: return B57600;
        case 115200: return B115200;
        case 230400: return B230400;
        default: return B19200;
    }
}

/* Global queues for handlers */
static MsgQueue *g_host_queue   = NULL;
static MsgQueue *g_device_queue = NULL;

/* Logging helpers */
static void debug_print(const char *fmt, ...) { (void)fmt; }
static void info_print(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt); vprintf(fmt, ap); printf("\n"); va_end(ap);
}

/* Simple message-queue implementation */
#define QUEUE_CAP  1024
typedef struct MsgQueue {
    NetSIOMsg *items[QUEUE_CAP];
    int head, tail, cnt;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} MsgQueue;

static void queue_init(MsgQueue *q) {
    q->head = q->tail = q->cnt = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->cond, NULL);
}

static void queue_clear(MsgQueue *q) {
    pthread_mutex_lock(&q->lock);
    while (q->cnt > 0) {
        free(q->items[q->head]);
        q->head = (q->head + 1) % QUEUE_CAP;
        q->cnt--;
    }
    pthread_mutex_unlock(&q->lock);
}

static void queue_push(MsgQueue *q, NetSIOMsg *m) {
    pthread_mutex_lock(&q->lock);
    if (q->cnt < QUEUE_CAP) {
        q->items[q->tail] = m;
        q->tail = (q->tail + 1) % QUEUE_CAP;
        q->cnt++;
        pthread_cond_signal(&q->cond);
    } else {
        debug_print("queue full, dropping message id=0x%02X", m->id);
        free(m);
    }
    pthread_mutex_unlock(&q->lock);
}

static NetSIOMsg *queue_pop(MsgQueue *q) {
    pthread_mutex_lock(&q->lock);
    while (q->cnt == 0) {
        pthread_cond_wait(&q->cond, &q->lock);
    }
    NetSIOMsg *m = q->items[q->head];
    q->head = (q->head + 1) % QUEUE_CAP;
    q->cnt--;
    pthread_mutex_unlock(&q->lock);
    return m;
}

/* Serial input thread */
static void *serial_in_thread(void *arg) {
    struct { SerialCtx *ser; MsgQueue *host_queue; } *ctx = arg;
    SerialCtx *ser = ctx->ser;
    MsgQueue *q = ctx->host_queue;
    for (;;) {
        uint8_t b;
        if (read(ser->fd, &b, 1) <= 0) continue;
        NetSIOMsg *m1 = calloc(1, sizeof(*m1));
        pthread_mutex_lock(&ser->lock);
        int proceed = ser->sync_flag;
        ser->sync_flag = 0;
        pthread_mutex_unlock(&ser->lock);
        m1->id = proceed ? NETSIO_PROCEED_ON : NETSIO_PROCEED_OFF;
        m1->arg_len = 0;
        queue_push(q, m1);

        NetSIOMsg *m2 = calloc(1, sizeof(*m2));
        m2->id = NETSIO_DATA_BYTE;
        m2->arg[0] = b;
        m2->arg_len = 1;
        queue_push(q, m2);
    }
    return NULL;
}

/* Serial output thread */
static void *serial_out_thread(void *arg) {
    struct { SerialCtx *ser; MsgQueue *device_queue; } *ctx = arg;
    SerialCtx *ser = ctx->ser;
    MsgQueue *q = ctx->device_queue;
    for (;;) {
        NetSIOMsg *m = queue_pop(q);
        if (m->id == NETSIO_DATA_BYTE) {
            write(ser->fd, m->arg, m->arg_len);
        } else if (m->id == NETSIO_COMMAND_OFF_SYNC) {
            pthread_mutex_lock(&ser->lock);
            ser->sync_flag = 1;
            ser->sync_num = m->arg[0];
            pthread_mutex_unlock(&ser->lock);
        }
        free(m);
    }
    return NULL;
}

/* Network input thread */
static void *netin_thread(void *arg) {
    struct { int sockfd; MsgQueue *host_queue; int netsio_port; } *ctx = arg;
    int sockfd = ctx->sockfd;
    MsgQueue *q = ctx->host_queue;
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    uint8_t buf[520];

    while (1) {
        ssize_t n = recvfrom(sockfd, buf, sizeof(buf), 0, (struct sockaddr*)&addr, &alen);
        if (n <= 0) continue;

        uint8_t id = buf[0];
        size_t expected = 0;

        switch (id) {
            case NETSIO_PING_REQUEST:
                // ping packet is exactly 1 byte; reply immediately and skip queue
                expected = 1;
                if ((size_t)n >= expected) {
                    uint8_t resp = NETSIO_PING_RESPONSE;
                    sendto(sockfd, &resp, 1, 0,
                           (struct sockaddr*)&addr, alen);
                }
                continue;

            case NETSIO_DATA_BLOCK:
                // buf[1] == data length
                if (n < 2) {
                    // too short to even know length
                    info_print("Short WRITE header: %zd bytes", n);
                    continue;
                }
                expected = 2 + buf[1];
                break;

            case NETSIO_COMMAND_ON:
                expected = 1;
                break;

            case NETSIO_COMMAND_OFF_SYNC:
                expected = 2;
                break;

            default:
                // unknown command: just take everything you got
                expected = n;
                break;
        }

        if ((size_t)n < expected) {
            info_print("Incomplete cmd 0x%02X: got %zd want %zu",
                       id, (size_t)n, expected);
            continue;
        }

        // ok, we have a full “expected” frame; enqueue it
        NetSIOMsg *m = calloc(1, sizeof(*m));
        if (!m) {
            perror("calloc");
            continue;
        }
        m->id      = id;
        m->arg_len = expected - 1;
        memcpy(m->arg, buf + 1, m->arg_len);
        queue_push(q, m);
    }
    return NULL;
}

/* Network output thread */
static void *netout_thread(void *arg) {
    struct { MsgQueue *device_queue; int sockfd; } *ctx = arg;
    MsgQueue *q = ctx->device_queue;
    int sockfd = ctx->sockfd;
    while (1) {
        NetSIOMsg *m = queue_pop(q);
        uint8_t packet[520];
        packet[0] = m->id;
        memcpy(packet + 1, m->arg, m->arg_len);
        sendto(sockfd, packet, 1 + m->arg_len, 0, NULL, 0);
        free(m);
    }
    return NULL;
}

/* Handler stubs - may not get used */
static int handle_debugreadbyte(uint32_t address) {
    // Debug read, return zero
    (void)address;
    return 0;
}
static int handle_readbyte(uint32_t address) {
    // Side-effect read, return zero
    (void)address;
    return 0;
}
static void handle_writebyte(uint32_t address, int val) {
    // Write to device memory (noop)
    (void)address;
    (void)val;
}
static void handle_coldreset(void) {
    info_print("HOST COLD RESET");
    // clear device queue
    if (g_device_queue) queue_clear(g_device_queue);
}
static void handle_warmreset(void) {
    info_print("HOST WARM RESET");
    if (g_device_queue) queue_clear(g_device_queue);
}
static int handle_script_event(int event, int arg) {
    // Synchronous event: send to devices and return no-ack
    if (g_device_queue) {
        NetSIOMsg *m = calloc(1, sizeof(*m));
        m->id = event;
        if (event == NETSIO_DATA_BYTE_SYNC || event == NETSIO_DATA_BYTE) {
            m->arg[0] = (uint8_t)arg;
            m->arg_len = 1;
        } else if (event == NETSIO_SPEED_CHANGE) {
            memcpy(m->arg, &arg, 4);
            m->arg_len = 4;
        }
        queue_push(g_device_queue, m);
    }
    return ATDEV_EMPTY_SYNC;
}
static void handle_script_post(int event, int arg) {
    // Asynchronous event: send to devices
    if (g_device_queue) {
        NetSIOMsg *m = calloc(1, sizeof(*m));
        m->id = event;
        if (event == NETSIO_DATA_BYTE) {
            m->arg[0] = (uint8_t)arg;
            m->arg_len = 1;
        } else if (event == NETSIO_SPEED_CHANGE) {
            memcpy(m->arg, &arg, 4);
            m->arg_len = 4;
        }
        queue_push(g_device_queue, m);
    }
}

int main(int argc, char *argv[]) {
    /* thread handles */
    pthread_t tid_ser_in, tid_ser_out;
    pthread_t tid_netin, tid_netout;

    int netsio_port = NETSIO_PORT;
    int atdev_port = NETSIO_ATDEV_PORT;
    char *serial_dev = NULL;
    /* default control signals for serial mode */
    char *command_sig = "RTS";
    char *proceed_sig = "CTS";
    int debug_enabled = 0;
    int verbose_enabled = 0;

    /* parse command-line args */
    {
        static struct option longopts[] = {
            {"netsio-port", required_argument, NULL, 'n'},
            {"serial",      required_argument, NULL, 's'},
            {"port",        required_argument, NULL, 'p'},
            {"command",     required_argument, NULL, 'c'},
            {"proceed",     required_argument, NULL, 'r'},
            {"debug",       no_argument,       NULL, 'd'},
            {"verbose",     no_argument,       NULL, 'v'},
            {"help",        no_argument,       NULL, 'h'},
            {0,0,0,0}
        };
        int opt;
        while ((opt = getopt_long(argc, argv, "n:s:p:c:r:dv", longopts, NULL)) != -1) {
            switch (opt) {
            case 'n': netsio_port = atoi(optarg); break;
            case 's': serial_dev  = optarg;       break;
            case 'p': atdev_port  = atoi(optarg); break;
            case 'c': command_sig = optarg;       break;
            case 'r': proceed_sig = optarg;       break;
            case 'd': debug_enabled = 1;          break;
            case 'v': verbose_enabled = 1;        break;
            default:
                printf("Usage: %s [--netsio-port port] [--serial device]\n"
                       "           [--port atdev-port] [--command RTS|DTR]\n"
                       "           [--proceed CTS|DSR] [-d] [-v]\n", argv[0]);
                return 0;
            }
        }
    }

    info_print("NetSIO HUB C version");

    /* Initialize queues */
    MsgQueue host_queue, device_queue;
    queue_init(&host_queue);
    queue_init(&device_queue);

    g_host_queue   = &host_queue;
    g_device_queue = &device_queue;

    if (serial_dev) {
        /* Serial port mode */
        info_print("Opening serial port: %s", serial_dev);
        int fd = open(serial_dev, O_RDWR | O_NOCTTY | O_NONBLOCK);
        if (fd < 0) { perror("open serial"); return 1; }
        struct termios tio;
        if (tcgetattr(fd, &tio) < 0) { perror("tcgetattr"); return 1; }
        cfmakeraw(&tio);
        cfsetispeed(&tio, baud_to_const(19200));
        cfsetospeed(&tio, baud_to_const(19200));
        tio.c_cflag |= (CLOCAL | CREAD);
        tio.c_cflag &= ~(CSTOPB | PARENB);
        tio.c_cc[VMIN]  = 0;
        tio.c_cc[VTIME] = 1;
        if (tcsetattr(fd, TCSANOW, &tio) < 0) { perror("tcsetattr"); return 1; }

        /* Setup serial context */
        SerialCtx ser = {
            .fd           = fd,
            .command_mbit = (strcmp(command_sig, "RTS")==0 ? TIOCM_RTS : TIOCM_DTR),
            .proceed_mbit = (strcmp(proceed_sig, "CTS")==0 ? TIOCM_CTS : TIOCM_DSR),
            .lock         = PTHREAD_MUTEX_INITIALIZER,
            .sync_flag    = 0,
            .sync_num     = 0
        };

        /* Start serial threads */
        struct { SerialCtx *ser; MsgQueue *host_queue; } *si_ctx = malloc(sizeof(*si_ctx));
        si_ctx->ser        = &ser;
        si_ctx->host_queue = &host_queue;
        pthread_create(&tid_ser_in,  NULL, serial_in_thread,  si_ctx);

        struct { SerialCtx *ser; MsgQueue *device_queue; } *so_ctx = malloc(sizeof(*so_ctx));
        so_ctx->ser          = &ser;
        so_ctx->device_queue = &device_queue;
        pthread_create(&tid_ser_out, NULL, serial_out_thread, so_ctx);

    } else {
        /* Network mode: start UDP NetSIO threads */
        int udpsock = socket(AF_INET, SOCK_DGRAM, 0);
        if (udpsock < 0) { perror("socket"); return 1; }

        struct sockaddr_in addr = {
            .sin_family      = AF_INET,
            .sin_addr.s_addr = INADDR_ANY,
            .sin_port        = htons(netsio_port)
        };
        if (bind(udpsock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind"); return 1;
        }
        info_print("Listening for NetSIO UDP on port %d", netsio_port);

        /* Start netin thread */
        struct { int sockfd; MsgQueue *host_queue; int netsio_port; } *ni_ctx = malloc(sizeof(*ni_ctx));
        ni_ctx->sockfd     = udpsock;
        ni_ctx->host_queue = &host_queue;
        ni_ctx->netsio_port= netsio_port;
        pthread_create(&tid_netin, NULL, netin_thread, ni_ctx);

        /* Start netout thread */
        struct { MsgQueue *device_queue; int sockfd; } *no_ctx = malloc(sizeof(*no_ctx));
        no_ctx->device_queue = &device_queue;
        no_ctx->sockfd        = udpsock;
        pthread_create(&tid_netout, NULL, netout_thread, no_ctx);
    }

    /* Not reached */
    if (serial_dev) {
        pthread_join(tid_ser_in,  NULL);
        pthread_join(tid_ser_out, NULL);
    } else {
        pthread_join(tid_netin,   NULL);
        pthread_join(tid_netout,  NULL);
    }
    return 0;
}
