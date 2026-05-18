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
#include <jansson.h>
#include <uuid/uuid.h>

#define BUFFER_SIZE 4096
#define UUID_LEN 37

char WORKER_UUID[UUID_LEN] = "Worker_1";
char MASTER_HOST[256] = "127.0.0.1";
int MASTER_PORT = 7070;
char ORIGINAL_MASTER_ADDR[256] = "";  // Para registrar em register_temporary_worker
double HEARTBEAT_INTERVAL = 6.0;
double RECONNECT_DELAY = 5.0;

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
    printf("%s [%-12s INFO] ", timestamp, WORKER_UUID);
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

// Processa command_redirect do master atual
void handle_command_redirect(int sock, const char* new_master_address) {
    log_info("[REDIRECT] Recebido command_redirect para %s", new_master_address);

    // Extrair host:port
    char new_host[256];
    int new_port;
    sscanf(new_master_address, "%[^:]:%d", new_host, &new_port);

    // Encerrar conexão atual graciosamente
    close(sock);

    // Conectar ao novo master
    int new_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(new_port);
    inet_pton(AF_INET, new_host, &addr.sin_addr);

    if (connect(new_sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        log_info("[REDIRECT] Conectado ao novo master %s:%d", new_host, new_port);

        // Enviar register_temporary_worker
        json_t* payload = json_object();
        json_object_set_new(payload, "worker_id", json_string(WORKER_UUID));
        json_object_set_new(payload, "original_master_address",
                           json_string(ORIGINAL_MASTER_ADDR[0] ? ORIGINAL_MASTER_ADDR : MASTER_HOST));

        char req_id[UUID_LEN];
        generate_uuid(req_id);
        json_t* msg = json_object();
        json_object_set_new(msg, "type", json_string("register_temporary_worker"));
        json_object_set_new(msg, "request_id", json_string(req_id));
        json_object_set(msg, "payload", payload);

        send_json(new_sock, msg);
        json_decref(msg);
        json_decref(payload);

        // Continuar operação normal neste novo master
        // (simplificado: aqui o worker entraria em loop normal)
    }
}

// Processa command_release (voltar ao master original)
void handle_command_release(int sock, const char* original_master_address) {
    log_info("[RELEASE] Recebido command_release, retornando ao master original: %s",
             original_master_address);
    close(sock);

    char orig_host[256];
    int orig_port;
    sscanf(original_master_address, "%[^:]:%d", orig_host, &orig_port);

    // Reconectar ao master original
    int new_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(orig_port);
    inet_pton(AF_INET, orig_host, &addr.sin_addr);

    if (connect(new_sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        log_info("[RELEASE] Reconectado ao master original %s:%d", orig_host, orig_port);
        // Continuar operação normal
    }
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    // Carregar variáveis
    const char* env;
    env = getenv("WORKER_UUID"); if (env) strncpy(WORKER_UUID, env, UUID_LEN-1);
    env = getenv("MASTER_HOST"); if (env) strncpy(MASTER_HOST, env, 255);
    env = getenv("MASTER_PORT"); if (env) MASTER_PORT = atoi(env);
    strcpy(ORIGINAL_MASTER_ADDR, MASTER_HOST);

    log_info("Worker %s iniciado, master original: %s:%d",
             WORKER_UUID, MASTER_HOST, MASTER_PORT);

    // Loop principal
    while (1) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(MASTER_PORT);
        inet_pton(AF_INET, MASTER_HOST, &addr.sin_addr);

        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            log_info("Conectado ao master %s:%d", MASTER_HOST, MASTER_PORT);

            char buffer[BUFFER_SIZE];
            int n = recv(sock, buffer, sizeof(buffer)-1, 0);
            if (n > 0) {
                buffer[n] = '\0';
                json_error_t error;
                json_t* msg = json_loads(buffer, 0, &error);
                if (msg) {
                    const char* type = json_string_value(json_object_get(msg, "type"));
                    json_t* payload = json_object_get(msg, "payload");

                    if (type && strcmp(type, "command_redirect") == 0) {
                        const char* new_addr = json_string_value(json_object_get(payload, "new_master_address"));
                        if (new_addr) handle_command_redirect(sock, new_addr);
                    } else if (type && strcmp(type, "command_release") == 0) {
                        const char* orig_addr = json_string_value(json_object_get(payload, "original_master_address"));
                        if (orig_addr) handle_command_release(sock, orig_addr);
                    }
                    json_decref(msg);
                }
            }
        }
        close(sock);
        sleep(RECONNECT_DELAY);
    }

    return 0;
}
