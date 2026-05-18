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
#include <jansson.h>
#include <uuid/uuid.h>

#define BUFFER_SIZE 4096
#define MAX_WORKERS 100
#define MAX_TASKS 1000
#define UUID_LEN 37
#define REQUEST_TIMEOUT_SEC 5

// Variáveis de ambiente (com defaults)
char MASTER_UUID[UUID_LEN] = "Master_A";
char HOST[256] = "0.0.0.0";
int PORT = 7070;
int SATURATION_THRESHOLD = 100;
double RELEASE_RATIO = 0.6;
double TASK_GEN_INTERVAL = 2.0;
double LOAD_REPORT_INTERVAL = 5.0;

// Estruturas para peers
typedef struct {
    char master_id[UUID_LEN];
    char host[256];
    int port;
    int socket_fd;
    pthread_mutex_t sock_lock;
} PeerInfo;

// Estruturas para workers
typedef struct WorkerInfo {
    char worker_id[UUID_LEN];
    int socket_fd;
    struct sockaddr_in addr;
    int busy;
    int borrowed;
    char original_master[UUID_LEN];
    char current_master[UUID_LEN];
    int tasks_done;
    struct WorkerInfo* next;
} WorkerInfo;

// Tarefa simulada
typedef struct SimTask {
    char task_id[UUID_LEN];
    json_t* payload;
    char assigned_to[UUID_LEN];
    struct SimTask* next;
} SimTask;

// Estrutura para controle de requisições pendentes
typedef struct PendingRequest {
    char request_id[UUID_LEN];
    char target_master_id[UUID_LEN];
    time_t sent_time;
    int workers_needed;
    struct PendingRequest* next;
} PendingRequest;

// Globais
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t pending_lock = PTHREAD_MUTEX_INITIALIZER;
WorkerInfo* workers = NULL;
SimTask* task_queue = NULL;
SimTask* task_queue_tail = NULL;
PendingRequest* pending_requests = NULL;
int completed_tasks = 0;
int worker_count = 0;
int borrowed_workers_count = 0;
int task_queue_size = 0;

// Peers
PeerInfo peers[10];
int peer_count = 0;

// Protótipos
void generate_uuid_v4(char* buf);
void log_info(const char* format, ...);
void log_warning(const char* format, ...);
void log_error(const char* format, ...);
void log_debug(const char* format, ...);
void send_json(int fd, json_t* obj);
int connect_to_peer(PeerInfo* peer);
void send_request_help(int peer_idx, int workers_needed);
void handle_request_help(int client_fd, const char* request_id, json_t* payload);
void handle_register_temporary_worker(int client_fd, json_t* payload);
void release_borrowed_workers();
int get_release_threshold();
int calculate_workers_needed(int current_load);

// ========== UTILITÁRIOS ==========
void generate_uuid_v4(char* buf) {
    uuid_t u;
    uuid_generate(u);
    uuid_unparse(u, buf);
}

void log_info(const char* format, ...) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);
    printf("%s [%-12s INFO] ", timestamp, MASTER_UUID);
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
}

void log_warning(const char* format, ...) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);
    printf("%s [%-12s WARN] ", timestamp, MASTER_UUID);
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
}

void log_error(const char* format, ...) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);
    printf("%s [%-12s ERROR] ", timestamp, MASTER_UUID);
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
}

void log_debug(const char* format, ...) {
#ifdef DEBUG
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);
    printf("%s [%-12s DEBUG] ", timestamp, MASTER_UUID);
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
#endif
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

// ========== TAREFA 01: Conexão TCP entre Masters ==========
int connect_to_peer(PeerInfo* peer) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer->port);
    inet_pton(AF_INET, peer->host, &addr.sin_addr);

    struct timeval timeout;
    timeout.tv_sec = REQUEST_TIMEOUT_SEC;
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    pthread_mutex_lock(&peer->sock_lock);
    if (peer->socket_fd != -1) close(peer->socket_fd);
    peer->socket_fd = sock;
    pthread_mutex_unlock(&peer->sock_lock);

    return sock;
}

// ========== TAREFA 02: Detecção de Saturação ==========
int get_release_threshold() {
    return (int)(SATURATION_THRESHOLD * RELEASE_RATIO);
}

int calculate_workers_needed(int current_load) {
    if (current_load <= SATURATION_THRESHOLD) return 0;
    int excess = current_load - SATURATION_THRESHOLD;
    int needed = (excess + 9) / 10;
    if (needed < 1) needed = 1;
    if (needed > 5) needed = 5;
    return needed;
}

// ========== TAREFA 03: Protocolo de Negociação ==========
void send_request_help(int peer_idx, int workers_needed) {
    char request_id[UUID_LEN];
    generate_uuid_v4(request_id);

    json_t* payload = json_object();
    json_object_set_new(payload, "master_id", json_string(MASTER_UUID));
    json_object_set_new(payload, "current_load", json_integer(task_queue_size));
    json_object_set_new(payload, "capacity", json_integer(SATURATION_THRESHOLD));
    json_object_set_new(payload, "workers_needed", json_integer(workers_needed));

    json_t* msg = json_object();
    json_object_set_new(msg, "type", json_string("request_help"));
    json_object_set_new(msg, "request_id", json_string(request_id));
    json_object_set(msg, "payload", payload);

    PendingRequest* pr = malloc(sizeof(PendingRequest));
    strcpy(pr->request_id, request_id);
    strcpy(pr->target_master_id, peers[peer_idx].master_id);
    pr->sent_time = time(NULL);
    pr->workers_needed = workers_needed;
    pthread_mutex_lock(&pending_lock);
    pr->next = pending_requests;
    pending_requests = pr;
    pthread_mutex_unlock(&pending_lock);

    log_info("[P2P] Enviando request_help para %s (need=%d, req_id=%s)",
             peers[peer_idx].master_id, workers_needed, request_id);

    int sock = connect_to_peer(&peers[peer_idx]);
    if (sock >= 0) {
        send_json(sock, msg);
        close(sock);
    } else {
        log_warning("[P2P] Nao foi possivel conectar ao peer %s", peers[peer_idx].master_id);
    }

    json_decref(msg);
    json_decref(payload);
}

void handle_request_help(int client_fd, const char* request_id, json_t* payload) {
    const char* requester_id = json_string_value(json_object_get(payload, "master_id"));
    int workers_needed = json_integer_value(json_object_get(payload, "workers_needed"));

    pthread_mutex_lock(&lock);
    int idle_workers = 0;
    WorkerInfo* w = workers;
    while (w) {
        if (!w->busy && !w->borrowed) idle_workers++;
        w = w->next;
    }
    int current_load = task_queue_size;
    pthread_mutex_unlock(&lock);

    json_t* response_payload = json_object();
    json_t* response;

    if (current_load < SATURATION_THRESHOLD * 0.7 && idle_workers >= workers_needed) {
        json_object_set_new(response_payload, "workers_offered", json_integer(workers_needed));

        json_t* worker_details = json_array();
        int offered = 0;
        pthread_mutex_lock(&lock);
        w = workers;
        while (w && offered < workers_needed) {
            if (!w->busy && !w->borrowed) {
                w->borrowed = 1;
                strcpy(w->original_master, MASTER_UUID);
                offered++;

                json_t* detail = json_object();
                json_object_set_new(detail, "id", json_string(w->worker_id));
                char addr_str[64];
                snprintf(addr_str, sizeof(addr_str), "%s:%d",
                         inet_ntoa(w->addr.sin_addr), ntohs(w->addr.sin_port));
                json_object_set_new(detail, "address", json_string(addr_str));
                json_array_append_new(worker_details, detail);

                json_t* redirect_payload = json_object();
                char new_master_addr[256];
                snprintf(new_master_addr, sizeof(new_master_addr), "%s:%d", requester_id, PORT);
                json_object_set_new(redirect_payload, "new_master_address", json_string(new_master_addr));
                json_t* redirect_msg = json_object();
                json_object_set_new(redirect_msg, "type", json_string("command_redirect"));
                json_object_set_new(redirect_msg, "request_id", json_string(""));
                json_object_set(redirect_msg, "payload", redirect_payload);
                send_json(w->socket_fd, redirect_msg);
                json_decref(redirect_msg);
                json_decref(redirect_payload);

                log_info("[P2P] Enviado command_redirect para Worker %s -> %s",
                         w->worker_id, requester_id);
            }
            w = w->next;
        }
        pthread_mutex_unlock(&lock);
        json_object_set_new(response_payload, "worker_details", worker_details);

        response = json_object();
        json_object_set_new(response, "type", json_string("response_accepted"));
        json_object_set_new(response, "request_id", json_string(request_id));
        json_object_set(response, "payload", response_payload);

        log_info("[P2P] Aceito pedido de %s, oferecendo %d workers",
                 requester_id, offered);
    } else {
        const char* reason = (current_load >= SATURATION_THRESHOLD) ? "high_load" : "no_workers_available";
        json_object_set_new(response_payload, "reason", json_string(reason));
        response = json_object();
        json_object_set_new(response, "type", json_string("response_rejected"));
        json_object_set_new(response, "request_id", json_string(request_id));
        json_object_set(response, "payload", response_payload);
        log_info("[P2P] Recusado pedido de %s: %s", requester_id, reason);
    }

    send_json(client_fd, response);
    json_decref(response);
    json_decref(response_payload);
}

void handle_response_accepted(const char* request_id, json_t* payload) {
    log_info("[P2P] response_accepted recebido (req_id=%s)", request_id);
    pthread_mutex_lock(&pending_lock);
    PendingRequest *prev = NULL, *curr = pending_requests;
    while (curr) {
        if (strcmp(curr->request_id, request_id) == 0) {
            if (prev) prev->next = curr->next;
            else pending_requests = curr->next;
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&pending_lock);
}

void handle_response_rejected(const char* request_id, json_t* payload) {
    const char* reason = json_string_value(json_object_get(payload, "reason"));
    log_warning("[P2P] response_rejected recebido (req_id=%s, reason=%s)",
                request_id, reason ? reason : "unknown");
    pthread_mutex_lock(&pending_lock);
    PendingRequest *prev = NULL, *curr = pending_requests;
    while (curr) {
        if (strcmp(curr->request_id, request_id) == 0) {
            if (prev) prev->next = curr->next;
            else pending_requests = curr->next;
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&pending_lock);
}

// ========== TAREFA 04: Register Temporary Worker ==========
void handle_register_temporary_worker(int client_fd, json_t* payload) {
    const char* worker_id = json_string_value(json_object_get(payload, "worker_id"));
    const char* original_master_addr = json_string_value(json_object_get(payload, "original_master_address"));

    pthread_mutex_lock(&lock);
    WorkerInfo* w = workers;
    while (w) {
        if (strcmp(w->worker_id, worker_id) == 0) {
            w->borrowed = 1;
            strcpy(w->original_master, original_master_addr ? original_master_addr : "unknown");
            strcpy(w->current_master, MASTER_UUID);
            borrowed_workers_count++;
            log_info("[P2P] Worker emprestado %s registrado (origem: %s)",
                     worker_id, w->original_master);
            break;
        }
    }
    pthread_mutex_unlock(&lock);

    json_t* ack = json_object();
    json_object_set_new(ack, "type", json_string("register_ack"));
    send_json(client_fd, ack);
    json_decref(ack);
}

// ========== TAREFA 05: Devolução do Worker ==========
void release_borrowed_workers() {
    pthread_mutex_lock(&lock);
    WorkerInfo* w = workers;
    while (w) {
        if (w->borrowed && strlen(w->original_master) > 0) {
            json_t* payload = json_object();
            json_object_set_new(payload, "original_master_address", json_string(w->original_master));
            json_t* msg = json_object();
            json_object_set_new(msg, "type", json_string("command_release"));
            json_object_set_new(msg, "request_id", json_string(""));
            json_object_set(msg, "payload", payload);
            send_json(w->socket_fd, msg);
            json_decref(msg);
            json_decref(payload);

            for (int i = 0; i < peer_count; i++) {
                json_t* notify_payload = json_object();
                json_object_set_new(notify_payload, "worker_id", json_string(w->worker_id));
                json_t* notify_msg = json_object();
                json_object_set_new(notify_msg, "type", json_string("notify_worker_returned"));
                json_object_set_new(notify_msg, "request_id", json_string(""));
                json_object_set(notify_msg, "payload", notify_payload);

                int sock = connect_to_peer(&peers[i]);
                if (sock >= 0) {
                    send_json(sock, notify_msg);
                    close(sock);
                }
                json_decref(notify_msg);
                json_decref(notify_payload);
            }

            w->borrowed = 0;
            w->original_master[0] = '\0';
            borrowed_workers_count--;
            log_info("[P2P] Enviado command_release para Worker %s", w->worker_id);
        }
        w = w->next;
    }
    pthread_mutex_unlock(&lock);
}

// ========== HANDLERS ORIGINAIS ==========
typedef enum {
    TASK_HEARTBEAT,
    TASK_TASK_RESULT,
    TASK_WORKER_STATUS,
    TASK_BORROW_WORKER,
    TASK_PEER_HELLO,
    TASK_LOAD_REPORT,
    TASK_ASSIGN_TASK,
    TASK_WORKER_MIGRATE,
    TASK_BORROW_ACK,
    TASK_UNKNOWN
} TaskType;

typedef enum {
    RESPONSE_ALIVE,
    RESPONSE_ACK,
    RESPONSE_OK,
    RESPONSE_DENIED,
    RESPONSE_UNKNOWN_TASK
} ResponseType;

const char* task_to_string(TaskType task) {
    switch (task) {
        case TASK_HEARTBEAT: return "HEARTBEAT";
        case TASK_TASK_RESULT: return "TASK_RESULT";
        case TASK_WORKER_STATUS: return "WORKER_STATUS";
        case TASK_BORROW_WORKER: return "BORROW_WORKER";
        case TASK_PEER_HELLO: return "PEER_HELLO";
        case TASK_LOAD_REPORT: return "LOAD_REPORT";
        case TASK_ASSIGN_TASK: return "ASSIGN_TASK";
        case TASK_WORKER_MIGRATE: return "WORKER_MIGRATE";
        case TASK_BORROW_ACK: return "BORROW_ACK";
        default: return "UNKNOWN";
    }
}

TaskType string_to_task(const char* str) {
    if (!str) return TASK_UNKNOWN;
    if (strcmp(str, "HEARTBEAT") == 0) return TASK_HEARTBEAT;
    if (strcmp(str, "TASK_RESULT") == 0) return TASK_TASK_RESULT;
    if (strcmp(str, "WORKER_STATUS") == 0) return TASK_WORKER_STATUS;
    if (strcmp(str, "BORROW_WORKER") == 0) return TASK_BORROW_WORKER;
    if (strcmp(str, "PEER_HELLO") == 0) return TASK_PEER_HELLO;
    if (strcmp(str, "LOAD_REPORT") == 0) return TASK_LOAD_REPORT;
    return TASK_UNKNOWN;
}

const char* response_to_string(ResponseType resp) {
    switch (resp) {
        case RESPONSE_ALIVE: return "ALIVE";
        case RESPONSE_ACK: return "ACK";
        case RESPONSE_OK: return "OK";
        case RESPONSE_DENIED: return "DENIED";
        case RESPONSE_UNKNOWN_TASK: return "UNKNOWN_TASK";
        default: return "UNKNOWN";
    }
}

json_t* create_response(const char* server_uuid, TaskType task, ResponseType response) {
    json_t* obj = json_object();
    json_object_set_new(obj, "SERVER_UUID", json_string(server_uuid));
    json_object_set_new(obj, "TASK", json_string(task_to_string(task)));
    json_object_set_new(obj, "RESPONSE", json_string(response_to_string(response)));
    return obj;
}

json_t* handle_heartbeat(json_t* payload, const char* worker_id) {
    log_info("HEARTBEAT de worker %s", worker_id);
    return create_response(MASTER_UUID, TASK_HEARTBEAT, RESPONSE_ALIVE);
}

json_t* handle_task_result(json_t* payload, const char* worker_id) {
    const char* task_id = json_string_value(json_object_get(payload, "TASK_ID"));
    pthread_mutex_lock(&lock);
    completed_tasks++;
    WorkerInfo* w = workers;
    while (w) {
        if (strcmp(w->worker_id, worker_id) == 0) {
            w->busy = 0;
            w->tasks_done++;
            break;
        }
        w = w->next;
    }
    pthread_mutex_unlock(&lock);
    log_info("Tarefa '%s' concluída por '%s'", task_id ? task_id : "?", worker_id);
    return create_response(MASTER_UUID, TASK_TASK_RESULT, RESPONSE_ACK);
}

json_t* handle_worker_status(json_t* payload, const char* worker_id) {
    const char* status = json_string_value(json_object_get(payload, "STATUS"));
    log_debug("Status de '%s': %s", worker_id, status ? status : "unknown");
    return create_response(MASTER_UUID, TASK_WORKER_STATUS, RESPONSE_OK);
}

// ========== THREADS ==========
void* task_generator(void* arg) {
    const char* ops[] = {"COMPUTE_FIBONACCI", "SORT_ARRAY", "HASH_DATA", "PING_ENDPOINT", "COMPRESS_DATA"};
    while (1) {
        usleep(TASK_GEN_INTERVAL * 1000000);
        SimTask* task = malloc(sizeof(SimTask));
        generate_uuid_v4(task->task_id);
        task->payload = json_object();
        json_object_set_new(task->payload, "OP", json_string(ops[rand() % 5]));
        json_object_set_new(task->payload, "N", json_integer(rand() % 91 + 10));
        task->assigned_to[0] = '\0';
        task->next = NULL;
        pthread_mutex_lock(&lock);
        if (task_queue_tail) task_queue_tail->next = task;
        else task_queue = task;
        task_queue_tail = task;
        task_queue_size++;
        pthread_mutex_unlock(&lock);
        const char* op = json_string_value(json_object_get(task->payload, "OP"));
        log_info("Nova tarefa: %s (%s). Fila: %d", task->task_id, op, task_queue_size);
    }
    return NULL;
}

void* task_dispatcher(void* arg) {
    while (1) {
        usleep(500000);
        pthread_mutex_lock(&lock);
        if (!task_queue) {
            pthread_mutex_unlock(&lock);
            continue;
        }
        WorkerInfo* free_worker = NULL;
        WorkerInfo* w = workers;
        while (w) {
            if (!w->busy) {
                free_worker = w;
                break;
            }
            w = w->next;
        }
        if (!free_worker) {
            pthread_mutex_unlock(&lock);
            continue;
        }
        SimTask* task = task_queue;
        task_queue = task->next;
        if (!task_queue) task_queue_tail = NULL;
        task_queue_size--;
        free_worker->busy = 1;
        int worker_fd = free_worker->socket_fd;
        char worker_id[UUID_LEN];
        strcpy(worker_id, free_worker->worker_id);
        pthread_mutex_unlock(&lock);
        json_t* assign_msg = json_object();
        json_object_set_new(assign_msg, "SERVER_UUID", json_string(MASTER_UUID));
        json_object_set_new(assign_msg, "TASK", json_string(task_to_string(TASK_ASSIGN_TASK)));
        json_object_set_new(assign_msg, "TASK_ID", json_string(task->task_id));
        json_object_set(assign_msg, "PAYLOAD", task->payload);
        send_json(worker_fd, assign_msg);
        json_decref(assign_msg);
        log_info("Tarefa '%s' enviada para Worker '%s'", task->task_id, worker_id);
        free(task);
    }
    return NULL;
}

void* load_monitor(void* arg) {
    while (1) {
        sleep(LOAD_REPORT_INTERVAL);
        pthread_mutex_lock(&lock);
        int current_load = task_queue_size;
        int wcount = worker_count;
        int borrowed = borrowed_workers_count;
        pthread_mutex_unlock(&lock);

        log_info("Carga: %d tarefas | Workers: %d (%d emprestados) | Concluidas: %d",
                 current_load, wcount, borrowed, completed_tasks);

        if (current_load < get_release_threshold() && borrowed_workers_count > 0) {
            log_info("[P2P] Carga baixa (%d < %d). Devolvendo workers...",
                     current_load, get_release_threshold());
            release_borrowed_workers();
        }
        else if (current_load > SATURATION_THRESHOLD && peer_count > 0) {
            int needed = calculate_workers_needed(current_load);
            if (needed > 0) {
                log_warning("[P2P] SOBRECARGA! Load=%d, pedindo %d workers",
                            current_load, needed);
                for (int i = 0; i < peer_count; i++) {
                    send_request_help(i, needed);
                }
            }
        }

        time_t now = time(NULL);
        pthread_mutex_lock(&pending_lock);
        PendingRequest *prev = NULL, *curr = pending_requests;
        while (curr) {
            if (difftime(now, curr->sent_time) > REQUEST_TIMEOUT_SEC) {
                log_warning("[P2P] Timeout: request_id=%s para %s",
                            curr->request_id, curr->target_master_id);
                if (prev) prev->next = curr->next;
                else pending_requests = curr->next;
                free(curr);
                curr = prev ? prev->next : pending_requests;
            } else {
                prev = curr;
                curr = curr->next;
            }
        }
        pthread_mutex_unlock(&pending_lock);
    }
    return NULL;
}

// ========== HANDLE CLIENT ==========
typedef struct {
    char buffer[BUFFER_SIZE];
    int pos;
} LineBuffer;

void line_buffer_init(LineBuffer* lb) {
    memset(lb->buffer, 0, BUFFER_SIZE);
    lb->pos = 0;
}

void* handle_client(void* arg) {
    int client_fd = *(int*)arg;
    free(arg);
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    getpeername(client_fd, (struct sockaddr*)&addr, &addr_len);
    char worker_id[UUID_LEN] = {0};
    LineBuffer lb;
    line_buffer_init(&lb);
    log_info("Nova conexao de %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    while (1) {
        char chunk[BUFFER_SIZE];
        int n = recv(client_fd, chunk, sizeof(chunk) - 1, 0);
        if (n <= 0) break;
        chunk[n] = '\0';
        json_error_t error;
        json_t* payload = json_loads(chunk, 0, &error);
        if (!payload) continue;

        const char* msg_type = json_string_value(json_object_get(payload, "type"));
        if (msg_type) {
            const char* req_id = json_string_value(json_object_get(payload, "request_id"));
            json_t* msg_payload = json_object_get(payload, "payload");

            if (strcmp(msg_type, "request_help") == 0) {
                handle_request_help(client_fd, req_id, msg_payload);
            }
            else if (strcmp(msg_type, "response_accepted") == 0) {
                handle_response_accepted(req_id, msg_payload);
            }
            else if (strcmp(msg_type, "response_rejected") == 0) {
                handle_response_rejected(req_id, msg_payload);
            }
            else if (strcmp(msg_type, "register_temporary_worker") == 0) {
                handle_register_temporary_worker(client_fd, msg_payload);
            }
            else {
                log_warning("[P2P] Tipo desconhecido: %s", msg_type);
            }
            json_decref(payload);
            continue;
        }

        const char* task_str = json_string_value(json_object_get(payload, "TASK"));
        const char* sender = json_string_value(json_object_get(payload, "SERVER_UUID"));
        TaskType task = string_to_task(task_str);

        if (worker_id[0] == '\0' && task == TASK_HEARTBEAT) {
            strncpy(worker_id, sender, UUID_LEN - 1);
            pthread_mutex_lock(&lock);
            WorkerInfo* new_worker = malloc(sizeof(WorkerInfo));
            strcpy(new_worker->worker_id, worker_id);
            new_worker->socket_fd = client_fd;
            new_worker->addr = addr;
            new_worker->busy = 0;
            new_worker->borrowed = 0;
            new_worker->original_master[0] = '\0';
            new_worker->current_master[0] = '\0';
            new_worker->tasks_done = 0;
            new_worker->next = workers;
            workers = new_worker;
            worker_count++;
            pthread_mutex_unlock(&lock);
            log_info("Worker '%s' registrado. Total: %d", worker_id, worker_count);
        }

        json_t* response = NULL;
        const char* id = worker_id[0] ? worker_id : sender;
        switch (task) {
            case TASK_HEARTBEAT:
                response = handle_heartbeat(payload, id);
                break;
            case TASK_TASK_RESULT:
                response = handle_task_result(payload, id);
                break;
            case TASK_WORKER_STATUS:
                response = handle_worker_status(payload, id);
                break;
            default:
                response = create_response(MASTER_UUID, TASK_UNKNOWN, RESPONSE_UNKNOWN_TASK);
        }
        if (response) {
            send_json(client_fd, response);
            json_decref(response);
        }
        json_decref(payload);
    }
    close(client_fd);

    if (worker_id[0]) {
        pthread_mutex_lock(&lock);
        WorkerInfo *prev = NULL, *curr = workers;
        while (curr) {
            if (strcmp(curr->worker_id, worker_id) == 0) {
                if (prev) prev->next = curr->next;
                else workers = curr->next;
                free(curr);
                worker_count--;
                break;
            }
            prev = curr;
            curr = curr->next;
        }
        pthread_mutex_unlock(&lock);
        log_info("Worker '%s' desconectado. Total: %d", worker_id, worker_count);
    }
    return NULL;
}

// ========== MAIN ==========
void load_environment() {
    const char* env;
    env = getenv("MASTER_UUID"); if (env) strncpy(MASTER_UUID, env, UUID_LEN - 1);
    env = getenv("MASTER_HOST"); if (env) strncpy(HOST, env, 255);
    env = getenv("MASTER_PORT"); if (env) PORT = atoi(env);
    env = getenv("SATURATION_THRESHOLD"); if (env) SATURATION_THRESHOLD = atoi(env);
    env = getenv("RELEASE_RATIO"); if (env) RELEASE_RATIO = atof(env);
    env = getenv("TASK_GEN_INTERVAL"); if (env) TASK_GEN_INTERVAL = atof(env);
    env = getenv("LOAD_REPORT_INTERVAL"); if (env) LOAD_REPORT_INTERVAL = atof(env);

    env = getenv("MASTER_PEERS");
    if (env && strlen(env) > 0) {
        char* peers_copy = strdup(env);
        char* token = strtok(peers_copy, ",");
        while (token && peer_count < 10) {
            char* colon = strchr(token, ':');
            if (colon) {
                *colon = '\0';
                strncpy(peers[peer_count].master_id, token, UUID_LEN-1);
                strncpy(peers[peer_count].host, colon+1, 255);
                peers[peer_count].port = PORT;
                peers[peer_count].socket_fd = -1;
                pthread_mutex_init(&peers[peer_count].sock_lock, NULL);
                peer_count++;
            }
            token = strtok(NULL, ",");
        }
        free(peers_copy);
    }
}

int main() {
    srand(time(NULL));
    load_environment();

    log_info("==================================================");
    log_info("Master '%s' | %s:%d", MASTER_UUID, HOST, PORT);
    log_info("Threshold saturacao: %d | Liberacao: %d (%.0f%%)",
             SATURATION_THRESHOLD, get_release_threshold(), RELEASE_RATIO*100);
    log_info("Peers conhecidos: %d", peer_count);
    log_info("==================================================");

    pthread_t gen_thread, disp_thread, mon_thread;
    pthread_create(&gen_thread, NULL, task_generator, NULL);
    pthread_create(&disp_thread, NULL, task_dispatcher, NULL);
    pthread_create(&mon_thread, NULL, load_monitor, NULL);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 50) < 0) {
        perror("listen");
        return 1;
    }

    log_info("Servidor escutando em %s:%d", HOST, PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int* client_fd = malloc(sizeof(int));
        *client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);

        if (*client_fd < 0) {
            free(client_fd);
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        pthread_t client_thread;
        pthread_create(&client_thread, NULL, handle_client, client_fd);
        pthread_detach(client_thread);
    }

    close(server_fd);
    return 0;
}
