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

// Variáveis de ambiente
char MASTER_UUID[UUID_LEN] = "Master_A";
char HOST[256] = "0.0.0.0";
int PORT = 7070;
int SATURATION_THRESHOLD = 100;    // capacity
double RELEASE_RATIO = 0.6;        // 60% da capacity para liberação
double TASK_GEN_INTERVAL = 2.0;
double LOAD_REPORT_INTERVAL = 5.0;

// Estrutura para peers (vizinhos)
typedef struct {
    char master_id[UUID_LEN];
    char host[256];
    int port;
    int socket_fd;           // conexão mantida (pool)
    pthread_mutex_t sock_lock;
} PeerInfo;

PeerInfo peers[10];
int peer_count = 0;

// Worker info
typedef struct WorkerInfo {
    char worker_id[UUID_LEN];
    int socket_fd;
    struct sockaddr_in addr;
    int busy;
    int borrowed;            // 1 = emprestado para outro master
    char original_master[UUID_LEN];  // master de origem (se emprestado)
    char current_master[UUID_LEN];   // master atual
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

// Estrutura para controle de requisições pendentes (request_help)
typedef struct PendingRequest {
    char request_id[UUID_LEN];
    char target_master[UUID_LEN];
    time_t sent_time;
    int workers_needed;
    struct PendingRequest* next;
} PendingRequest;

// Globais
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
WorkerInfo* workers = NULL;
SimTask* task_queue = NULL;
SimTask* task_queue_tail = NULL;
PendingRequest* pending_requests = NULL;
int completed_tasks = 0;
int worker_count = 0;
int borrowed_workers_count = 0;
int task_queue_size = 0;

// Protótipos
void generate_uuid(char* buf);
void log_info(const char* format, ...);
void log_warning(const char* format, ...);
void log_error(const char* format, ...);
void send_json(int fd, json_t* obj);
json_t* create_json_message(const char* type, const char* request_id, json_t* payload);
void send_to_peer(int peer_idx, json_t* msg);

// ========== UTILITÁRIOS ==========
void generate_uuid(char* buf) {
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

json_t* create_json_message(const char* type, const char* request_id, json_t* payload) {
    json_t* msg = json_object();
    json_object_set_new(msg, "type", json_string(type));
    if (request_id) json_object_set_new(msg, "request_id", json_string(request_id));
    if (payload) json_object_set(msg, "payload", payload);
    else json_object_set_new(msg, "payload", json_object());
    return msg;
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

void send_to_peer(int peer_idx, json_t* msg) {
    PeerInfo* peer = &peers[peer_idx];
    pthread_mutex_lock(&peer->sock_lock);
    int fd = peer->socket_fd;
    if (fd == -1) {
        fd = connect_to_peer(peer);
    }
    if (fd != -1) {
        send_json(fd, msg);
    }
    pthread_mutex_unlock(&peer->sock_lock);
}

// ========== TAREFA 02: Detecção de Saturação ==========
int get_release_threshold() {
    return (int)(SATURATION_THRESHOLD * RELEASE_RATIO);
}

int calculate_workers_needed(int current_load) {
    if (current_load <= SATURATION_THRESHOLD) return 0;
    int excess = current_load - SATURATION_THRESHOLD;
    int needed = (excess + 9) / 10;  // cada worker resolve ~10 tarefas
    if (needed < 1) needed = 1;
    if (needed > 5) needed = 5;      // limite de workers emprestados por vez
    return needed;
}

// ========== TAREFA 03/04: Protocolo de Negociação ==========
void add_pending_request(const char* request_id, const char* target_master, int workers_needed) {
    PendingRequest* pr = malloc(sizeof(PendingRequest));
    strcpy(pr->request_id, request_id);
    strcpy(pr->target_master, target_master);
    pr->sent_time = time(NULL);
    pr->workers_needed = workers_needed;
    pr->next = pending_requests;
    pending_requests = pr;
}

void remove_pending_request(const char* request_id) {
    PendingRequest *prev = NULL, *curr = pending_requests;
    while (curr) {
        if (strcmp(curr->request_id, request_id) == 0) {
            if (prev) prev->next = curr->next;
            else pending_requests = curr->next;
            free(curr);
            return;
        }
        prev = curr;
        curr = curr->next;
    }
}

// Enviar request_help para um peer
int send_request_help(int peer_idx, int workers_needed) {
    char request_id[UUID_LEN];
    generate_uuid(request_id);

    json_t* payload = json_object();
    json_object_set_new(payload, "master_id", json_string(MASTER_UUID));
    json_object_set_new(payload, "current_load", json_integer(task_queue_size));
    json_object_set_new(payload, "capacity", json_integer(SATURATION_THRESHOLD));
    json_object_set_new(payload, "workers_needed", json_integer(workers_needed));

    json_t* msg = create_json_message("request_help", request_id, payload);

    log_info("[REQUEST] Enviando request_help para %s (need=%d, req_id=%s)",
             peers[peer_idx].master_id, workers_needed, request_id);

    add_pending_request(request_id, peers[peer_idx].master_id, workers_needed);
    send_to_peer(peer_idx, msg);
    json_decref(msg);
    json_decref(payload);

    return 0;
}

// Processar request_help recebido (Master B)
void handle_request_help(int client_fd, const char* request_id, json_t* payload) {
    const char* requester_id = json_string_value(json_object_get(payload, "master_id"));
    int workers_needed = json_integer_value(json_object_get(payload, "workers_needed"));

    pthread_mutex_lock(&lock);
    // Contar workers ociosos e não emprestados
    int idle_workers = 0;
    WorkerInfo* w = workers;
    while (w) {
        if (!w->busy && !w->borrowed) idle_workers++;
        w = w->next;
    }
    int current_load = task_queue_size;
    pthread_mutex_unlock(&lock);

    json_t* response_payload = json_object();
    json_t* msg = NULL;

    // Critério para aceitar: carga baixa e workers ociosos suficientes
    if (current_load < SATURATION_THRESHOLD * 0.7 && idle_workers >= workers_needed) {
        // response_accepted
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

                log_info("[HELP] Oferecendo Worker %s para empréstimo a %s",
                         w->worker_id, requester_id);
            }
            w = w->next;
        }
        pthread_mutex_unlock(&lock);

        json_object_set_new(response_payload, "worker_details", worker_details);
        msg = create_json_message("response_accepted", request_id, response_payload);

        // Enviar command_redirect para cada worker (Tarefa 04)
        pthread_mutex_lock(&lock);
        w = workers;
        while (w) {
            if (w->borrowed && strcmp(w->original_master, MASTER_UUID) == 0) {
                json_t* redirect_payload = json_object();
                char new_master_addr[256];
                snprintf(new_master_addr, sizeof(new_master_addr), "%s:%d", requester_id, PORT);
                json_object_set_new(redirect_payload, "new_master_address", json_string(new_master_addr));
                json_t* redirect_msg = create_json_message("command_redirect", NULL, redirect_payload);
                send_json(w->socket_fd, redirect_msg);
                json_decref(redirect_msg);
                json_decref(redirect_payload);
            }
            w = w->next;
        }
        pthread_mutex_unlock(&lock);
    } else {
        // response_rejected
        const char* reason = (current_load >= SATURATION_THRESHOLD) ? "high_load" : "no_workers_available";
        json_object_set_new(response_payload, "reason", json_string(reason));
        msg = create_json_message("response_rejected", request_id, response_payload);
        log_info("[HELP] Recusando pedido de %s: %s", requester_id, reason);
    }

    send_json(client_fd, msg);
    json_decref(msg);
    json_decref(response_payload);
}

// Processar response_accepted (Master A)
void handle_response_accepted(const char* request_id, json_t* payload) {
    log_info("[RESPONSE] response_accepted recebido (req_id=%s)", request_id);
    remove_pending_request(request_id);
    // Workers já estão sendo redirecionados via command_redirect
}

void handle_response_rejected(const char* request_id, json_t* payload) {
    const char* reason = json_string_value(json_object_get(payload, "reason"));
    log_warning("[RESPONSE] response_rejected recebido (req_id=%s, reason=%s)",
                request_id, reason ? reason : "unknown");
    remove_pending_request(request_id);
}

// ========== TAREFA 04: Register Temporary Worker ==========
void handle_register_temporary_worker(int client_fd, json_t* payload, const char* worker_id) {
    const char* original_master_addr = json_string_value(json_object_get(payload, "original_master_address"));

    pthread_mutex_lock(&lock);
    WorkerInfo* w = workers;
    while (w) {
        if (strcmp(w->worker_id, worker_id) == 0) {
            w->borrowed = 1;
            strcpy(w->original_master, original_master_addr ? original_master_addr : "unknown");
            strcpy(w->current_master, MASTER_UUID);
            borrowed_workers_count++;
            log_info("[REGISTER] Worker emprestado %s registrado (origem: %s)",
                     worker_id, w->original_master);
            break;
        }
        w = w->next;
    }
    pthread_mutex_unlock(&lock);
}

// ========== TAREFA 05: Devolução do Worker ==========
void release_borrowed_workers() {
    pthread_mutex_lock(&lock);
    WorkerInfo* w = workers;
    while (w) {
        if (w->borrowed && strcmp(w->current_master, MASTER_UUID) == 0) {
            // command_release
            json_t* payload = json_object();
            json_object_set_new(payload, "original_master_address", json_string(w->original_master));
            json_t* msg = create_json_message("command_release", NULL, payload);
            send_json(w->socket_fd, msg);
            json_decref(msg);
            json_decref(payload);

            log_info("[RELEASE] Enviando command_release para Worker %s", w->worker_id);

            // notify_worker_returned para o master original
            for (int i = 0; i < peer_count; i++) {
                json_t* notify_payload = json_object();
                json_object_set_new(notify_payload, "worker_id", json_string(w->worker_id));
                json_t* notify_msg = create_json_message("notify_worker_returned", NULL, notify_payload);
                send_to_peer(i, notify_msg);
                json_decref(notify_msg);
                json_decref(notify_payload);
            }

            w->borrowed = 0;
            w->original_master[0] = '\0';
            borrowed_workers_count--;
        }
        w = w->next;
    }
    pthread_mutex_unlock(&lock);
}

// ========== Threads ==========
void* load_monitor(void* arg) {
    while (1) {
        sleep(LOAD_REPORT_INTERVAL);

        pthread_mutex_lock(&lock);
        int current_load = task_queue_size;
        pthread_mutex_unlock(&lock);

        log_info("[MONITOR] Carga atual: %d / %d (threshold=%d, liberação=%d)",
                 current_load, SATURATION_THRESHOLD, SATURATION_THRESHOLD, get_release_threshold());

        // Verificar se precisa liberar workers (carga baixa)
        if (current_load < get_release_threshold() && borrowed_workers_count > 0) {
            log_info("[MONITOR] Carga abaixo do threshold de liberação. Devolvendo workers...");
            release_borrowed_workers();
        }

        // Verificar se precisa pedir ajuda (sobrecarga)
        if (current_load > SATURATION_THRESHOLD && peer_count > 0) {
            int needed = calculate_workers_needed(current_load);
            if (needed > 0) {
                log_warning("[MONITOR] SOBRECARGA! Load=%d, necessidade=%d workers",
                            current_load, needed);
                for (int i = 0; i < peer_count; i++) {
                    send_request_help(i, needed);
                }
            }
        }

        // Timeout checker para pedidos pendentes
        time_t now = time(NULL);
        PendingRequest *prev = NULL, *curr = pending_requests;
        while (curr) {
            if (difftime(now, curr->sent_time) > REQUEST_TIMEOUT_SEC) {
                log_warning("[TIMEOUT] request_id=%s expirado", curr->request_id);
                PendingRequest* to_free = curr;
                if (prev) prev->next = curr->next;
                else pending_requests = curr->next;
                curr = curr->next;
                free(to_free);
            } else {
                prev = curr;
                curr = curr->next;
            }
        }
    }
    return NULL;
}

// ========== Parser de mensagens Master-to-Master ==========
void process_master_message(int client_fd, json_t* msg) {
    const char* type = json_string_value(json_object_get(msg, "type"));
    const char* request_id = json_string_value(json_object_get(msg, "request_id"));
    json_t* payload = json_object_get(msg, "payload");

    if (!type) {
        log_warning("[MASTER-MSG] Mensagem sem campo 'type'");
        return;
    }

    log_info("[MASTER-MSG] type=%s, request_id=%s", type, request_id ? request_id : "(null)");

    if (strcmp(type, "request_help") == 0) {
        handle_request_help(client_fd, request_id, payload);
    } else if (strcmp(type, "response_accepted") == 0) {
        handle_response_accepted(request_id, payload);
    } else if (strcmp(type, "response_rejected") == 0) {
        handle_response_rejected(request_id, payload);
    } else if (strcmp(type, "notify_worker_returned") == 0) {
        const char* worker_id = json_string_value(json_object_get(payload, "worker_id"));
        log_info("[NOTIFY] Worker %s retornou ao master original", worker_id ? worker_id : "?");
    } else {
        log_warning("[MASTER-MSG] Tipo desconhecido: %s (ignorado)", type);
    }
}

// ========== Função principal ==========
int main() {
    srand(time(NULL));

    // Carregar variáveis de ambiente
    const char* env;
    env = getenv("MASTER_UUID"); if (env) strncpy(MASTER_UUID, env, UUID_LEN-1);
    env = getenv("MASTER_HOST"); if (env) strncpy(HOST, env, 255);
    env = getenv("MASTER_PORT"); if (env) PORT = atoi(env);
    env = getenv("SATURATION_THRESHOLD"); if (env) SATURATION_THRESHOLD = atoi(env);
    env = getenv("RELEASE_RATIO"); if (env) RELEASE_RATIO = atof(env);

    // Carregar peers
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
                peers[peer_count].port = PORT;  // mesmo porta para todos
                peers[peer_count].socket_fd = -1;
                pthread_mutex_init(&peers[peer_count].sock_lock, NULL);
                peer_count++;
            }
            token = strtok(NULL, ",");
        }
        free(peers_copy);
    }

    log_info("==================================================");
    log_info("Master '%s' iniciado em %s:%d", MASTER_UUID, HOST, PORT);
    log_info("Threshold saturação: %d | Liberação: %d (%.0f%%)",
             SATURATION_THRESHOLD, get_release_threshold(), RELEASE_RATIO*100);
    log_info("Peers conhecidos: %d", peer_count);
    log_info("==================================================");

    // Iniciar thread de monitoramento
    pthread_t mon_thread;
    pthread_create(&mon_thread, NULL, load_monitor, NULL);
    pthread_detach(mon_thread);

    // Socket server
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &addr.sin_addr);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 50) < 0) {
        perror("listen");
        return 1;
    }

    log_info("Servidor TCP escutando em %s:%d", HOST, PORT);

    // Loop principal de aceitação de conexões
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);

        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        log_info("Nova conexão de %s:%d", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        // Simples handler: ler linha JSON e processar
        char buffer[BUFFER_SIZE];
        int n = recv(client_fd, buffer, sizeof(buffer)-1, 0);
        if (n > 0) {
            buffer[n] = '\0';
            json_error_t error;
            json_t* msg = json_loads(buffer, 0, &error);
            if (msg) {
                process_master_message(client_fd, msg);
                json_decref(msg);
            }
        }
        close(client_fd);
    }

    close(server_fd);
    return 0;
}
