#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <ctype.h>
#include <math.h>
#include <jansson.h>
#include <openssl/sha.h>
#include <zlib.h>

#define BUFFER_SIZE 4096
#define UUID_LEN 37

// Variáveis de ambiente (com defaults)
char WORKER_UUID[UUID_LEN] = "Worker_A1";
char MASTER_HOST[256] = "127.0.0.1";
int MASTER_PORT = 7070;
double HEARTBEAT_INTERVAL = 6.0;
double RECONNECT_DELAY = 5.0;
double SOCKET_TIMEOUT = 10.0;

typedef enum {
    TASK_HEARTBEAT,
    TASK_ASSIGN_TASK,
    TASK_TASK_RESULT,
    TASK_WORKER_MIGRATE,
    TASK_MIGRATE_ACK,
    TASK_UNKNOWN
} TaskType;

typedef enum {
    RESPONSE_ALIVE,
    RESPONSE_ACK,
    RESPONSE_UNKNOWN
} ResponseType;

typedef struct {
    char host[256];
    int port;
} MigrationInfo;

typedef struct {
    char buffer[BUFFER_SIZE];
    int pos;
} LineBuffer;

const char* task_to_string(TaskType task);
TaskType string_to_task(const char* str);
const char* response_to_string(ResponseType resp);
void send_json(int fd, json_t* obj);
char* execute_task(json_t* payload);
double get_monotonic_time();

void log_message(const char* level, const char* format, ...) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);

    printf("%s [%-12s %s] ", timestamp, WORKER_UUID, level);

    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
}

#define log_info(...) log_message("INFO", __VA_ARGS__)
#define log_warning(...) log_message("WARNING", __VA_ARGS__)
#define log_error(...) log_message("ERROR", __VA_ARGS__)
#define log_debug(...) log_message("DEBUG", __VA_ARGS__)

double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void line_buffer_init(LineBuffer* lb) {
    memset(lb->buffer, 0, BUFFER_SIZE);
    lb->pos = 0;
}

json_t** line_buffer_feed(LineBuffer* lb, const char* data, int len, int* msg_count) {
    static json_t* messages[100];
    *msg_count = 0;

    for (int i = 0; i < len && lb->pos < BUFFER_SIZE - 1; i++) {
        lb->buffer[lb->pos++] = data[i];

        if (data[i] == '\n') {
            lb->buffer[lb->pos - 1] = '\0';
            json_error_t error;
            json_t* msg = json_loads(lb->buffer, 0, &error);
            if (msg) {
                messages[(*msg_count)++] = msg;
            }
            lb->pos = 0;
        }
    }
    return messages;
}

void send_json(int fd, json_t* obj) {
    char* json_str = json_dumps(obj, JSON_COMPACT);
    if (!json_str) return;

    size_t len = strlen(json_str);
    char* buffer = malloc(len + 2);
    snprintf(buffer, len + 2, "%s\n", json_str);

    send(fd, buffer, len + 1, 0);
    free(buffer);
    free(json_str);
}

long long compute_fibonacci(int n) {
    if (n <= 0) return 0;
    if (n == 1) return 1;

    long long a = 0, b = 1;
    for (int i = 2; i <= n; i++) {
        long long temp = b;
        b = a + b;
        a = temp;
    }
    return b;
}

char* sort_array(int n) {
    int* arr = malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        arr[i] = rand() % 1001;
    }

    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                int temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }

    char* result = malloc(256);
    if (n >= 3) {
        snprintf(result, 256, "[%d, %d, %d]...", arr[0], arr[1], arr[2]);
    } else {
        snprintf(result, 256, "[%d]", arr[0]);
    }
    free(arr);
    return result;
}

char* hash_data(int n) {
    unsigned char* data = malloc(n * 8);
    for (int i = 0; i < n * 8; i++) {
        data[i] = rand() % 256;
    }

    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(data, n * 8, hash);
    free(data);

    char* result = malloc(33);
    for (int i = 0; i < 8; i++) {
        sprintf(result + i * 2, "%02x", hash[i]);
    }
    result[16] = '\0';
    return result;
}

char* ping_endpoint() {
    int delay_ms = 20 + rand() % 181;
    usleep(delay_ms * 1000);

    char* result = malloc(64);
    snprintf(result, 64, "200 OK (%dms)", delay_ms);
    return result;
}

char* compress_data(int n) {
    size_t data_size = (n % 256) * (n / 10 + 1);
    unsigned char* data = malloc(data_size);

    for (size_t i = 0; i < data_size; i++) {
        data[i] = i % (n % 256);
    }

    uLongf compressed_size = compressBound(data_size);
    unsigned char* compressed = malloc(compressed_size);

    compress(compressed, &compressed_size, data, data_size);

    double ratio = (double)data_size / (compressed_size > 0 ? compressed_size : 1);

    char* result = malloc(64);
    snprintf(result, 64, "ratio=%.2f", ratio);

    free(data);
    free(compressed);
    return result;
}

char* execute_task(json_t* payload) {
    const char* op = json_string_value(json_object_get(payload, "OP"));
    int n = json_integer_value(json_object_get(payload, "N"));

    if (!op) op = "NOP";
    if (n <= 0) n = 10;

    char* result = NULL;

    if (strcmp(op, "COMPUTE_FIBONACCI") == 0) {
        long long fib = compute_fibonacci(n);
        result = malloc(32);
        snprintf(result, 32, "%lld", fib);
    }
    else if (strcmp(op, "SORT_ARRAY") == 0) {
        result = sort_array(n);
    }
    else if (strcmp(op, "HASH_DATA") == 0) {
        result = hash_data(n);
    }
    else if (strcmp(op, "PING_ENDPOINT") == 0) {
        result = ping_endpoint();
    }
    else if (strcmp(op, "COMPRESS_DATA") == 0) {
        result = compress_data(n);
    }
    else {
        usleep(100000);
        result = strdup("NOOP");
    }
    int sleep_ms = 200 + rand() % 1300;
    usleep(sleep_ms * 1000);

    return result;
}

int process_message(json_t* payload, int sock, MigrationInfo* migration) {
    const char* task_str = json_string_value(json_object_get(payload, "TASK"));
    TaskType task = string_to_task(task_str);

    if (task == TASK_HEARTBEAT) {
        const char* status = json_string_value(json_object_get(payload, "RESPONSE"));
        const char* server_uuid = json_string_value(json_object_get(payload, "SERVER_UUID"));

        if (status && strcmp(status, response_to_string(RESPONSE_ALIVE)) == 0) {
            log_info("ALIVE (Master '%s' ativo)", server_uuid ? server_uuid : "?");
        } else {
            log_warning("Resposta inesperada ao heartbeat: '%s'", status ? status : "?");
        }
    }
    else if (task == TASK_ASSIGN_TASK) {
        const char* task_id = json_string_value(json_object_get(payload, "TASK_ID"));
        json_t* tp = json_object_get(payload, "PAYLOAD");

        const char* op = json_string_value(json_object_get(tp, "OP"));
        int n = json_integer_value(json_object_get(tp, "N"));

        log_info("Tarefa recebida: %s  OP=%s  N=%d", task_id ? task_id : "?", op ? op : "?", n);

        char* result = execute_task(tp);
        log_info("Tarefa %s concluída. Resultado: %s", task_id ? task_id : "?", result);

        json_t* response = json_object();
        json_object_set_new(response, "SERVER_UUID", json_string(WORKER_UUID));
        json_object_set_new(response, "TASK", json_string(task_to_string(TASK_TASK_RESULT)));
        json_object_set_new(response, "TASK_ID", json_string(task_id ? task_id : ""));
        json_object_set_new(response, "RESULT", json_string(result));

        send_json(sock, response);
        json_decref(response);
        free(result);
    }
    else if (task == TASK_WORKER_MIGRATE) {
        const char* new_host = json_string_value(json_object_get(payload, "NEW_HOST"));
        int new_port = json_integer_value(json_object_get(payload, "NEW_PORT"));
        const char* owner = json_string_value(json_object_get(payload, "OWNER"));

        log_info("Instrução de migração recebida → %s:%d  (owner: %s)",
                 new_host ? new_host : "127.0.0.1", new_port, owner ? owner : "?");

        json_t* ack = json_object();
        json_object_set_new(ack, "SERVER_UUID", json_string(WORKER_UUID));
        json_object_set_new(ack, "TASK", json_string(task_to_string(TASK_MIGRATE_ACK)));
        json_object_set_new(ack, "RESPONSE", json_string(response_to_string(RESPONSE_ACK)));

        send_json(sock, ack);
        json_decref(ack);

        if (new_host) strcpy(migration->host, new_host);
        migration->port = new_port;

        return 0;
    }

    return 1;
}

MigrationInfo* connection_loop(const char* host, int port) {
    log_info("Conectando a %s:%d ...", host, port);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        log_error("Falha ao criar socket: %s", strerror(errno));
        return NULL;
    }

    // Configurar timeout
    struct timeval tv;
    tv.tv_sec = (int)SOCKET_TIMEOUT;
    tv.tv_usec = (SOCKET_TIMEOUT - tv.tv_sec) * 1000000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_error("Falha na conexão: %s", strerror(errno));
        close(sock);
        return NULL;
    }

    log_info("Conectado a %s:%d", host, port);

    LineBuffer lb;
    line_buffer_init(&lb);

    double last_heartbeat = 0.0;
    MigrationInfo* migration = malloc(sizeof(MigrationInfo));
    memset(migration, 0, sizeof(MigrationInfo));

    while (1) {
        double now = get_monotonic_time();

        // Enviar heartbeat
        if (now - last_heartbeat >= HEARTBEAT_INTERVAL) {
            log_info("Enviando HEARTBEAT → %s:%d ...", host, port);

            json_t* heartbeat = json_object();
            json_object_set_new(heartbeat, "SERVER_UUID", json_string(WORKER_UUID));
            json_object_set_new(heartbeat, "TASK", json_string(task_to_string(TASK_HEARTBEAT)));

            send_json(sock, heartbeat);
            json_decref(heartbeat);

            last_heartbeat = now;
        }

        // Configurar timeout curto para recv
        tv.tv_sec = 0;
        tv.tv_usec = 500000;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        char chunk[BUFFER_SIZE];
        int n = recv(sock, chunk, sizeof(chunk) - 1, 0);

        if (n == 0) {
            log_error("Conexão encerrada pelo Master.");
            free(migration);
            close(sock);
            return NULL;
        }

        if (n > 0) {
            int msg_count;
            json_t** messages = line_buffer_feed(&lb, chunk, n, &msg_count);

            for (int i = 0; i < msg_count; i++) {
                int continue_loop = process_message(messages[i], sock, migration);
                json_decref(messages[i]);

                if (!continue_loop) {
                    close(sock);
                    return migration;
                }
            }
        }
    }

    close(sock);
    free(migration);
    return NULL;
}

const char* task_to_string(TaskType task) {
    switch (task) {
        case TASK_HEARTBEAT: return "HEARTBEAT";
        case TASK_ASSIGN_TASK: return "ASSIGN_TASK";
        case TASK_TASK_RESULT: return "TASK_RESULT";
        case TASK_WORKER_MIGRATE: return "WORKER_MIGRATE";
        case TASK_MIGRATE_ACK: return "MIGRATE_ACK";
        default: return "UNKNOWN";
    }
}

TaskType string_to_task(const char* str) {
    if (!str) return TASK_UNKNOWN;
    if (strcmp(str, "HEARTBEAT") == 0) return TASK_HEARTBEAT;
    if (strcmp(str, "ASSIGN_TASK") == 0) return TASK_ASSIGN_TASK;
    if (strcmp(str, "TASK_RESULT") == 0) return TASK_TASK_RESULT;
    if (strcmp(str, "WORKER_MIGRATE") == 0) return TASK_WORKER_MIGRATE;
    if (strcmp(str, "MIGRATE_ACK") == 0) return TASK_MIGRATE_ACK;
    return TASK_UNKNOWN;
}

const char* response_to_string(ResponseType resp) {
    switch (resp) {
        case RESPONSE_ALIVE: return "ALIVE";
        case RESPONSE_ACK: return "ACK";
        default: return "UNKNOWN";
    }
}

void load_environment() {
    const char* env;

    env = getenv("WORKER_UUID");
    if (env) strncpy(WORKER_UUID, env, UUID_LEN - 1);

    env = getenv("MASTER_HOST");
    if (env) strncpy(MASTER_HOST, env, 255);

    env = getenv("MASTER_PORT");
    if (env) MASTER_PORT = atoi(env);

    env = getenv("HEARTBEAT_INTERVAL");
    if (env) HEARTBEAT_INTERVAL = atof(env);

    env = getenv("RECONNECT_DELAY");
    if (env) RECONNECT_DELAY = atof(env);

    env = getenv("SOCKET_TIMEOUT");
    if (env) SOCKET_TIMEOUT = atof(env);
}

void run_worker() {
    log_info("════════════════════════════════════════════════════════════");
    log_info("  Worker '%s'  |  Alvo inicial: %s:%d", WORKER_UUID, MASTER_HOST, MASTER_PORT);
    log_info("  Heartbeat a cada %.0fs  |  Reconexão em %.0fs", HEARTBEAT_INTERVAL, RECONNECT_DELAY);
    log_info("════════════════════════════════════════════════════════════");

    char current_host[256];
    strcpy(current_host, MASTER_HOST);
    int current_port = MASTER_PORT;

    while (1) {
        MigrationInfo* migration = connection_loop(current_host, current_port);

        if (migration && migration->host[0] != '\0') {
            // Migração para novo Master
            log_info("Migrando para novo Master: %s:%d", migration->host, migration->port);
            strcpy(current_host, migration->host);
            current_port = migration->port;
            free(migration);
        } else {
            // Falha ou desconexão - volta ao master original
            strcpy(current_host, MASTER_HOST);
            current_port = MASTER_PORT;
            log_info("Reconectando em %.0fs ...", RECONNECT_DELAY);
            sleep(RECONNECT_DELAY);

            if (migration) free(migration);
        }
    }
}

int main() {
    srand(time(NULL));
    load_environment();

    // Ignorar SIGPIPE para evitar crash em escritas em sockets fechados
    signal(SIGPIPE, SIG_IGN);

    run_worker();

    return 0;
}
