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

#define BUFFER_SIZE 4096
#define MAX_WORKERS 100
#define MAX_TASKS 1000
#define UUID_LEN 37

// Variáveis de ambiente (com defaults)
char MASTER_UUID[UUID_LEN] = "Master_A";
char HOST[256] = "0.0.0.0";
int PORT = 9000;
int OVERLOAD_THRESHOLD = 5;
double TASK_GEN_INTERVAL = 2.0;
double LOAD_REPORT_INTERVAL = 10.0;

// Estruturas para peers
typedef struct {
    char host[256];
    int port;
} PeerAddress;

PeerAddress PEER_ADDRESSES[10];
int PEER_COUNT = 0;

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

typedef struct WorkerInfo {
    char worker_id[UUID_LEN];
    int socket_fd;
    struct sockaddr_in addr;
    int busy;
    int borrowed;
    char owner[UUID_LEN];
    int tasks_done;
    struct WorkerInfo* next;
} WorkerInfo;

typedef struct SimTask {
    char task_id[UUID_LEN];
    json_t* payload;
    char assigned_to[UUID_LEN];
    struct SimTask* next;
} SimTask;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
WorkerInfo* workers = NULL;
SimTask* task_queue = NULL;
SimTask* task_queue_tail = NULL;
int completed_tasks = 0;
int worker_count = 0;
int task_queue_size = 0;

const char* task_to_string(TaskType task);
TaskType string_to_task(const char* str);
const char* response_to_string(ResponseType resp);
void send_json(int fd, json_t* obj);
json_t* create_response(const char* server_uuid, TaskType task, ResponseType response);

void generate_uuid(char* buf) {
    snprintf(buf, 9, "%08x", rand() ^ time(NULL));
}

void log_message(const char* level, const char* format, ...) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm);

    printf("%s [%-12s %s] ", timestamp, MASTER_UUID, level);

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

json_t* handle_heartbeat(json_t* payload, const char* worker_id) {
    const char* server_uuid = json_string_value(json_object_get(payload, "SERVER_UUID"));
    log_info("HEARTBEAT de '%s'", server_uuid ? server_uuid : "?");

    json_t* response = json_object();
    json_object_set_new(response, "SERVER_UUID", json_string(MASTER_UUID));
    json_object_set_new(response, "TASK", json_string(task_to_string(TASK_HEARTBEAT)));
    json_object_set_new(response, "RESPONSE", json_string(response_to_string(RESPONSE_ALIVE)));
    return response;
}

json_t* handle_task_result(json_t* payload, const char* worker_id) {
    const char* task_id = json_string_value(json_object_get(payload, "TASK_ID"));
    json_t* result = json_object_get(payload, "RESULT");

    pthread_mutex_lock(&lock);
    completed_tasks++;

    WorkerInfo* w = workers;
    while (w) {
        if (strcmp(w->worker_id, worker_id) == 0) {
            w->busy = 0;
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

json_t* handle_borrow_worker(json_t* payload, const char* _) {
    const char* requesting_master = json_string_value(json_object_get(payload, "SERVER_UUID"));
    const char* target_host = json_string_value(json_object_get(payload, "REDIRECT_HOST"));
    int target_port = json_integer_value(json_object_get(payload, "REDIRECT_PORT"));

    pthread_mutex_lock(&lock);

    // Encontrar worker livre não emprestado
    WorkerInfo* candidate = workers;
    while (candidate) {
        if (!candidate->busy && !candidate->borrowed) break;
        candidate = candidate->next;
    }

    if (candidate) {
        candidate->borrowed = 1;
        strcpy(candidate->owner, MASTER_UUID);
        char wid[UUID_LEN];
        strcpy(wid, candidate->worker_id);

        log_info("↗  Emprestando Worker '%s' para Master '%s'", wid, requesting_master);

        // Enviar mensagem de migração para o worker
        json_t* migrate_msg = json_object();
        json_object_set_new(migrate_msg, "SERVER_UUID", json_string(MASTER_UUID));
        json_object_set_new(migrate_msg, "TASK", json_string(task_to_string(TASK_WORKER_MIGRATE)));
        json_object_set_new(migrate_msg, "NEW_HOST", json_string(target_host));
        json_object_set_new(migrate_msg, "NEW_PORT", json_integer(target_port));
        json_object_set_new(migrate_msg, "OWNER", json_string(MASTER_UUID));

        send_json(candidate->socket_fd, migrate_msg);
        json_decref(migrate_msg);
        pthread_mutex_unlock(&lock);

        json_t* response = create_response(MASTER_UUID, TASK_BORROW_ACK, RESPONSE_OK);
        json_object_set_new(response, "WORKER_ID", json_string(wid));
        return response;
    } else {
        pthread_mutex_unlock(&lock);
        log_info("↘  Nenhum Worker disponível para emprestar a '%s'", requesting_master);
        return create_response(MASTER_UUID, TASK_BORROW_ACK, RESPONSE_DENIED);
    }
}

json_t* handle_peer_hello(json_t* payload, const char* _) {
    const char* peer_id = json_string_value(json_object_get(payload, "SERVER_UUID"));
    log_info("Peer '%s' se apresentou.", peer_id ? peer_id : "?");

    json_t* response = create_response(MASTER_UUID, TASK_PEER_HELLO, RESPONSE_ACK);
    json_object_set_new(response, "LOAD", json_integer(task_queue_size));
    json_object_set_new(response, "WORKERS", json_integer(worker_count));
    return response;
}

json_t* handle_load_report(json_t* payload, const char* _) {
    const char* peer_id = json_string_value(json_object_get(payload, "SERVER_UUID"));
    int load = json_integer_value(json_object_get(payload, "LOAD"));
    log_info("Carga reportada por '%s': %d tarefas", peer_id ? peer_id : "?", load);
    return create_response(MASTER_UUID, TASK_LOAD_REPORT, RESPONSE_ACK);
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

json_t* create_response(const char* server_uuid, TaskType task, ResponseType response) {
    json_t* obj = json_object();
    json_object_set_new(obj, "SERVER_UUID", json_string(server_uuid));
    json_object_set_new(obj, "TASK", json_string(task_to_string(task)));
    json_object_set_new(obj, "RESPONSE", json_string(response_to_string(response)));
    return obj;
}

typedef struct {
    char buffer[BUFFER_SIZE];
    int pos;
} LineBuffer;

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

void* handle_client(void* arg) {
    int client_fd = *(int*)arg;
    free(arg);

    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    getpeername(client_fd, (struct sockaddr*)&addr, &addr_len);

    char worker_id[UUID_LEN] = {0};
    LineBuffer lb;
    line_buffer_init(&lb);

    log_info("⬆  Nova conexão de %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    while (1) {
        char chunk[BUFFER_SIZE];
        int n = recv(client_fd, chunk, sizeof(chunk) - 1, 0);
        if (n <= 0) break;

        int msg_count;
        json_t** messages = line_buffer_feed(&lb, chunk, n, &msg_count);

        for (int i = 0; i < msg_count; i++) {
            json_t* payload = messages[i];
            const char* task_str = json_string_value(json_object_get(payload, "TASK"));
            const char* sender = json_string_value(json_object_get(payload, "SERVER_UUID"));

            TaskType task = string_to_task(task_str);

            // Registrar worker no primeiro heartbeat
            if (worker_id[0] == '\0' && task == TASK_HEARTBEAT) {
                strncpy(worker_id, sender, UUID_LEN - 1);

                pthread_mutex_lock(&lock);
                WorkerInfo* new_worker = malloc(sizeof(WorkerInfo));
                strcpy(new_worker->worker_id, worker_id);
                new_worker->socket_fd = client_fd;
                new_worker->addr = addr;
                new_worker->busy = 0;
                new_worker->borrowed = 0;
                new_worker->owner[0] = '\0';
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
                case TASK_BORROW_WORKER:
                    response = handle_borrow_worker(payload, id);
                    break;
                case TASK_PEER_HELLO:
                    response = handle_peer_hello(payload, id);
                    break;
                case TASK_LOAD_REPORT:
                    response = handle_load_report(payload, id);
                    break;
                default:
                    log_warning("Task desconhecida '%s' de %s", task_str ? task_str : "?", id);
                    response = create_response(MASTER_UUID, TASK_UNKNOWN, RESPONSE_UNKNOWN_TASK);
            }

            if (response) {
                send_json(client_fd, response);
                json_decref(response);
            }
            json_decref(payload);
        }
    }

    close(client_fd);

    // Remover worker
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

void* task_generator(void* arg) {
    const char* ops[] = {"COMPUTE_FIBONACCI", "SORT_ARRAY", "HASH_DATA", "PING_ENDPOINT", "COMPRESS_DATA"};

    while (1) {
        usleep(TASK_GEN_INTERVAL * 1000000);

        SimTask* task = malloc(sizeof(SimTask));
        generate_uuid(task->task_id);
        task->payload = json_object();
        json_object_set_new(task->payload, "OP", json_string(ops[rand() % 5]));
        json_object_set_new(task->payload, "N", json_integer(rand() % 91 + 10));
        task->assigned_to[0] = '\0';
        task->next = NULL;

        pthread_mutex_lock(&lock);
        if (task_queue_tail) {
            task_queue_tail->next = task;
        } else {
            task_queue = task;
        }
        task_queue_tail = task;
        task_queue_size++;
        pthread_mutex_unlock(&lock);

        const char* op = json_string_value(json_object_get(task->payload, "OP"));
        log_info("Nova tarefa gerada: %s (%s). Fila: %d", task->task_id, op, task_queue_size);
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

        // Encontrar worker livre
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
        pthread_mutex_unlock(&lock);

        log_info("Carga atual: %d tarefas | %d workers ativos | %d concluídas",
                 current_load, wcount, completed_tasks);

        if (current_load >= OVERLOAD_THRESHOLD && PEER_COUNT > 0) {
            log_warning("SOBRECARGA DETECTADA (%d >= %d). Iniciando negociação P2P...",
                       current_load, OVERLOAD_THRESHOLD);

            // Tentar pegar worker emprestado de peers
            for (int i = 0; i < PEER_COUNT; i++) {
                log_info("Contactando peer %s:%d para empréstimo de Worker...",
                        PEER_ADDRESSES[i].host, PEER_ADDRESSES[i].port);

                int sock = socket(AF_INET, SOCK_STREAM, 0);
                if (sock < 0) continue;

                struct sockaddr_in peer_addr;
                peer_addr.sin_family = AF_INET;
                peer_addr.sin_port = htons(PEER_ADDRESSES[i].port);
                inet_pton(AF_INET, PEER_ADDRESSES[i].host, &peer_addr.sin_addr);

                struct timeval timeout;
                timeout.tv_sec = 5;
                timeout.tv_usec = 0;
                setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

                if (connect(sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) == 0) {
                    json_t* request = json_object();
                    json_object_set_new(request, "SERVER_UUID", json_string(MASTER_UUID));
                    json_object_set_new(request, "TASK", json_string(task_to_string(TASK_BORROW_WORKER)));

                    const char* redirect_host = strcmp(HOST, "0.0.0.0") == 0 ? "127.0.0.1" : HOST;
                    json_object_set_new(request, "REDIRECT_HOST", json_string(redirect_host));
                    json_object_set_new(request, "REDIRECT_PORT", json_integer(PORT));

                    send_json(sock, request);
                    json_decref(request);

                    char buffer[BUFFER_SIZE];
                    int n = recv(sock, buffer, sizeof(buffer) - 1, 0);
                    if (n > 0) {
                        buffer[n] = '\0';
                        json_error_t error;
                        json_t* resp = json_loads(buffer, 0, &error);
                        if (resp) {
                            const char* response = json_string_value(json_object_get(resp, "RESPONSE"));
                            if (response && strcmp(response, "OK") == 0) {
                                log_info("Empréstimo aceito por peer");
                                json_decref(resp);
                                close(sock);
                                break;
                            }
                            json_decref(resp);
                        }
                    }
                }
                close(sock);
            }
        }
    }
    return NULL;
}

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

void load_environment() {
    const char* env;

    env = getenv("MASTER_UUID");
    if (env) strncpy(MASTER_UUID, env, UUID_LEN - 1);

    env = getenv("MASTER_HOST");
    if (env) strncpy(HOST, env, 255);

    env = getenv("MASTER_PORT");
    if (env) PORT = atoi(env);

    env = getenv("OVERLOAD_THRESHOLD");
    if (env) OVERLOAD_THRESHOLD = atoi(env);

    env = getenv("TASK_GEN_INTERVAL");
    if (env) TASK_GEN_INTERVAL = atof(env);

    env = getenv("LOAD_REPORT_INTERVAL");
    if (env) LOAD_REPORT_INTERVAL = atof(env);

    env = getenv("MASTER_PEERS");
    if (env && strlen(env) > 0) {
        char* peers_copy = strdup(env);
        char* token = strtok(peers_copy, ",");
        while (token && PEER_COUNT < 10) {
            char* colon = strchr(token, ':');
            if (colon) {
                *colon = '\0';
                strncpy(PEER_ADDRESSES[PEER_COUNT].host, token, 255);
                PEER_ADDRESSES[PEER_COUNT].port = atoi(colon + 1);
                PEER_COUNT++;
            }
            token = strtok(NULL, ",");
        }
        free(peers_copy);
    }
}

int main() {
    srand(time(NULL));
    load_environment();

    log_info("════════════════════════════════════════════════════════════");
    log_info("  Master '%s'  |  %s:%d", MASTER_UUID, HOST, PORT);
    log_info("  Threshold de sobrecarga: %d tarefas", OVERLOAD_THRESHOLD);
    log_info("  Peers conhecidos: %d", PEER_COUNT);
    log_info("════════════════════════════════════════════════════════════");

    // Iniciar threads
    pthread_t gen_thread, disp_thread, mon_thread;
    pthread_create(&gen_thread, NULL, task_generator, NULL);
    pthread_create(&disp_thread, NULL, task_dispatcher, NULL);
    pthread_create(&mon_thread, NULL, load_monitor, NULL);

    log_info("Threads iniciadas.");

    // Configurar socket do servidor
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

    log_info("Master escutando em %s:%d ...", HOST, PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int* client_fd = malloc(sizeof(int));
        *client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);

        if (*client_fd < 0) {
            free(client_fd);
            if (errno == EINTR) break;
            perror("accept");
            continue;
        }

        pthread_t client_thread;
        pthread_create(&client_thread, NULL, handle_client, client_fd);
        pthread_detach(client_thread);

        log_debug("Thread para %s:%d iniciada",
                  inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    }

    close(server_fd);
    return 0;
}
