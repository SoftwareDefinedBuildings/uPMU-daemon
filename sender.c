#define EVENT_BUF_LEN 128
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define ADDRESSD "127.0.0.1" // for bw.cal-sdb.org, this should be "54.241.13.58"
#define ADDRESSP 1883

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/socket.h>
#include <arpa/inet.h>

typedef struct 
{
    int fd;
    char* path;
} watched_entry_t;

watched_entry_t child;

fd_set set;

int send_file(struct sockaddr* server_addr, const char* filepath)
{
    // Ignore files that are not .dat
    const char* substr = filepath + strlen(filepath) - 4;
    if (strcmp(substr, ".dat") != 0)
    {
        printf("skipping over file that is not \".dat\"\n");
        return 0;
    }
    
    // Find file path up to file's parent directory
    // Works assuming there is a parent directory (/.../file.dat)
    char filepathAfterParent[256];
    char filepathCopy[256];
    strcpy(filepathCopy, filepath);
    strcpy(filepathAfterParent, basename(dirname(filepathCopy)));
    strcat(filepathAfterParent, "/");
    strcpy(filepathCopy, filepath); // since basename() may have changed it
    strcat(filepathAfterParent, basename(filepathCopy));
    uint32_t size = strlen(filepathAfterParent);
    
    // Initialize connection
    int socket_descriptor = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket_descriptor < 0)
    {
        perror("could not create socket");
        exit(1);
    }
    if (connect(socket_descriptor, server_addr, sizeof(*server_addr)) < 0)
    {
        perror("could not connect");
        close(socket_descriptor);
        exit(1);
    }
    
    // Open file
    FILE *input;
    input = fopen(filepath, "rb");
    if (input == NULL) {
        printf("Error: cannot read file %s.\n", filepath);
        return 1;
    }
    
    // Get the length of the file
    fseek(input, 0, SEEK_END);
    uint32_t length = ftell(input);
    
    // Store length of filename, length of data, and filename in data array
    // The null terminator may be overwritten; the length of the filename does not
    // include the null terminator.
    // Space is added to the end of filename so it is word-aligned.
    uint32_t size_word = (size & 0xFFFFFFFC) + 4; // size with extra bytes so it's word-aligned
    uint8_t data[8 + size_word + length] __attribute__((aligned(4)));
    *((uint32_t*) data) = size;
    *((uint32_t*) (data + 4)) = length;
    strcpy(data + 8, filepathAfterParent);
    uint8_t* datastart = data + 8 + size_word;
    rewind(input);
    
    // Read data into array
    int32_t dataread = fread(datastart, 1, length, input);
    if (dataread != length) {
        printf("Error: could not finish reading file (read %d out of %d bytes)\n", dataread, length);
        return 1;
    }
    fclose(input);
    
    // Send message over TCP
    int result = write(socket_descriptor, data, 8 + size_word + length);
    
    // Close the socket
    shutdown(socket_descriptor, SHUT_RDWR);
    close(socket_descriptor);
    return result;
}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        printf("Usage: %s <directorytowatch>\n", argv[0]);
        exit(1);
    }
    
    // Create the socket address
    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_port = htons(ADDRESSP);
    int result = inet_pton(AF_INET, ADDRESSD, &server.sin_addr);
    if (result < 0) {
        perror("invalid address family in \"inet_pton\"");
        exit(1);
    } else if (result == 0) {
        perror("invalid ip address in \"inet_pton\"");
        exit(1);
    }
    
    // Look for file/directory additions
    int fd = inotify_init();
    if (fd < 0)
    {
        perror("inotify_init");
        exit(1);
    }
    
    //This watch will notice any new directories
    inotify_add_watch(fd, argv[1], IN_CREATE);
    
    char buffer[EVENT_BUF_LEN];
    char ndirname[256];

    struct timeval timeout;
    int rlen;
    int i;

    child.fd = -1;

    while(1)
    {
        //Select changes the timeout and the set
        FD_ZERO(&set);
        FD_SET(fd, &set);
        timeout.tv_sec = 2; //Do a wavelet keepalive every 3 seconds
        timeout.tv_usec = 0;
        rlen = select(fd + 1, &set, NULL, NULL, &timeout);
        if (rlen == 0)
        {
            // no activity
            continue;
        }
        rlen = read(fd, buffer, EVENT_BUF_LEN);
        i = 0;
        if (rlen == -1) {
            printf("Error (possibly caused by filepath that is too long)\n");
        }
        while (i < rlen)
        {
            struct inotify_event* ev = (struct inotify_event*) &buffer[i];
            if (ev->len)
            {
                /* Check for a new directory */
                if ((IN_CREATE & ev->mask) && (IN_ISDIR & ev->mask))
                {
                    /* Check for new files within the directory */
                    strncpy(ndirname, argv[1], sizeof(ndirname)-1);
                    strncat(ndirname, "/", sizeof(ndirname)-1);
                    strncat(ndirname, ev->name, sizeof(ndirname)-1);
                    watched_entry_t et = {.fd = inotify_add_watch(fd, ndirname, IN_CLOSE_WRITE),
                                          .path = ndirname};
                    if (child.fd != -1)
                    {
                        if (inotify_rm_watch(fd, child.fd))
                        {
                            perror("RM watch");
                        }
                        child.fd = -1;
                    }
                    child = et;
                    
                    //Ok great, but we may have missed some files, so let's check for them:
                    DIR *basedir;
                    struct dirent *dir;
                    basedir = opendir(ndirname);
                    if (!basedir)
                    {
                        perror("basedir");
                        exit(1);
                    }
                    while ((dir = readdir(basedir)) != NULL)
                    {
                        if (dir->d_name[0] == '.')
                            continue;
                        char fullname [256];
                        printf("Checking out appeared file\n");
                        strcpy(fullname, ndirname);
                        strcat(fullname,"/");
                        strcat(fullname, dir->d_name);
                        send_file((struct sockaddr*) &server, fullname);
                    }
                    closedir(basedir);
                }
                /* Check for a new file */
                else if (((IN_CLOSE_WRITE) & ev->mask) && !(IN_ISDIR & ev->mask))
                {
                    if (child.fd == ev->wd)
                    {
                        char fullname [256];
                        printf("File closed after writing: %s/%s\n",child.path, ev->name);
                        strcpy(fullname, child.path);
                        strcat(fullname,"/");
                        strcat(fullname, ev->name);
                        printf("Processing\n");
                        send_file((struct sockaddr*) &server, fullname);
                        printf("Processing done\n");
                    }
                    else
                    {
                        fprintf(stderr, "Error, got unexpected file close FD\n");
                    }
                }
            }
            i += EVENT_SIZE + ev->len;
        }
    }
}
