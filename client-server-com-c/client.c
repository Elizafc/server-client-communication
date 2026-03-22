#include <bits/sockaddr.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 7070

int main(){
    int sock;
    struct sockaddr_in server_addr;
    char buffer[1024] = {0};
    char *msg = "Hi!";

    sock = socket(AF_INET, SOCK_STREAM, 0);
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
    send(sock, msg, strlen(msg), 0);
    read(sock, buffer, 1024);
    printf("Reply: %s\n", buffer);
    close(sock);

    return 0;
}
