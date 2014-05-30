#define EVENT_BUF_LEN 128 * ( sizeof (struct inotify_event) )
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define ADDRESSP 1883
#define NUMTRIES 10 // the number of times to try sending a file until giving up

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <arpa/inet.h>

typedef struct 
{
    int fd;
    char* path;
} watched_entry_t;

watched_entry_t child;

fd_set set;

// when processing directories upon initialization, store the watched directories in an array
int num_watched_dirs = 0;
int size_watched_arr = 0;

// the socket descriptor
int socket_des = -1;

// 1 if connected (i.e., socket connection needs to be closed), 0 otherwise
int connected = 0;

// pointer to the server address
struct sockaddr* server_addr;

uint32_t sendid = 0;

/* Close the socket connection. */
void close_connection(int socket_descriptor)
{
    shutdown(socket_descriptor, SHUT_RDWR);
    close(socket_descriptor);
}

/* Exit, closing the socket connection if necessary. */
void safe_exit(int arg)
{
    if (connected)
    {
        close_connection(socket_des);
    }
    exit(arg);
}

int send_until_success(int* socket_descriptor, const char* filepath, int inRootDir)
{
    int numTries = 0;
    int result;
    while ((result = send_file(*socket_descriptor, filepath, inRootDir)) == -1 && numTries < NUMTRIES)
    {
        connected = 0;
        if (numTries == 0)
        {
            close_connection(*socket_descriptor);
        }
        numTries++;
        printf("Attempting to reconnect (attempt #%d)\n", numTries);
        *socket_descriptor = make_socket();
        connected = 1;
    }
    if (numTries == NUMTRIES)
    {
        connected = 0;
        exit(1);
    }
    return result;
}

int send_file(int socket_descriptor, const char* filepath, int inRootDir)
{
    printf("Sending file %s\n", filepath);
    
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
    if (inRootDir)
    {
        strcpy(filepathAfterParent, basename(filepathCopy));
    }
    else
    {
        strcpy(filepathAfterParent, basename(dirname(filepathCopy)));
        strcat(filepathAfterParent, "/");
        strcpy(filepathCopy, filepath); // since basename() may have changed it
        strcat(filepathAfterParent, basename(filepathCopy));
    }
    uint32_t size = strlen(filepathAfterParent);
    
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
    
    // Store file number (sendid), length of filename, length of data, and filename in data array
    // The null terminator may be overwritten; the length of the filename does not
    // include the null terminator.
    // Space is added to the end of filename so it is word-aligned.
    uint32_t size_word = (size & 0xFFFFFFFC) + 4; // size with extra bytes so it's word-aligned
    uint8_t data[12 + size_word + length] __attribute__((aligned(4)));
    *((uint64_t*) data) = sendid;
    *((uint32_t*) (data + 4)) = size;
    *((uint32_t*) (data + 8)) = length;
    strcpy(data + 12, filepathAfterParent);
    uint8_t* datastart = data + 12 + size_word;
    rewind(input);
    
    // Read data into array
    int32_t dataread = fread(datastart, 1, length, input);
    if (dataread != length) {
        printf("Error: could not finish reading file (read %d out of %d bytes)\n", dataread, length);
        return 1;
    }
    fclose(input);
    
    // Send message over TCP
    int dataleft = 12 + size_word + length;
    int datawritten;
    uint8_t* dest = data;
    while (dataleft > 0)
    {
        int datawritten = write(socket_descriptor, dest, 12 + size_word + length);
        if (datawritten < 0)
        {
            printf("Could not send file %s\n", filepath);
            return -1;
        }
        dataleft -= datawritten;
        dest += datawritten;
    }

    uint32_t response;
    
    // Get confirmation of receipt
    printf("Waiting for confirmation of receipt...\n");
    
    dataleft = 4;
    while (dataleft > 0)
    {
        dataread = read(socket_descriptor, ((uint8_t*) &response) + 4 - dataleft, 4);
        if (dataread < 0)
        {
            printf("Could not receive confirmation of receipt of %s\n", filepath);
            return -1;
        }
        else if (dataread == 0)
        {
            printf("Connection was closed before confirmation was received\n");
            return -1;
        }
        dataleft -= dataread;
    }
    
    if (response != sendid++)
    {
        printf("Received improper confirmation of receipt of %s (got %d)\n", filepath, response);
        return -1;
    }
    else
    {
        printf("Received confirmation, deleting file\n");
        // Delete the file
        if (unlink(filepath) != 0)
        {
            printf("Could not delete %s\n", filepath);
        }
    }
    return 0;
}

int processdir(const char* dirpath, int* socket_descriptor, int depth)
{
    int numsubdirs = 0;
    if (depth >= 2) // Only look in subdirectories, not in subsubdirectories, etc.
    {
        return 0;
    }
    DIR* directory = opendir(dirpath);
    if (directory == NULL)
    {
        printf("%s is not a valid directory\n", dirpath);
        return -1;
    }
    struct dirent* subdir;
    struct stat pathStats;
    errno = 0;
    while ((subdir = readdir(directory)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            char fullpath[strlen(dirpath) + strlen(subdir->d_name) + 2];
            strcpy(fullpath, dirpath);
            strcat(fullpath, "/");
            strcat(fullpath, subdir->d_name);
            if (stat(fullpath, &pathStats) != 0)
            {
                printf("Could not read file %s\n", fullpath);
                return -1;
            }
            if (S_ISDIR(pathStats.st_mode))
            {
                if (processdir(fullpath, socket_descriptor, depth + 1) < 0)
                {
                    return -1;
                }
                numsubdirs++;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                send_until_success(socket_descriptor, fullpath, depth == 0);
            }
        }
    }
    if (errno != 0)
    {
        printf("Could not finish reading directory %s\n", dirpath);
        return -1;
    }
    return numsubdirs;
}

int make_socket()
{
    int socket_descriptor = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket_descriptor < 0)
    {
        perror("could not create socket");
        safe_exit(1);
    }
    if (connect(socket_descriptor, server_addr, sizeof(*server_addr)) < 0)
    {
        perror("could not connect");
        close(socket_descriptor);
        return -1;
    }
    return socket_descriptor;
}

void interrupt_handler(int sig)
{
    safe_exit(0);
}

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        printf("Usage: interrupt_handler%s <directorytowatch> <targetserver>\n", argv[0]);
        safe_exit(1);
    }
    
    // Set up signal to handle Ctrl-C (close socket connection before terminating)
    struct sigaction action;
    action.sa_handler = interrupt_handler;
    action.sa_flags = 0;
    sigemptyset(&action.sa_mask);
    if (-1 == sigaction(SIGINT, &action, NULL))
    {
        printf("Could not set up signal to handle keyboard interrupt\n");
        exit(1);
    }
    
    // Create the socket address
    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_port = htons(ADDRESSP);
    int result = inet_pton(AF_INET, argv[2], &server.sin_addr);
    if (result < 0) {
        perror("invalid address family in \"inet_pton\"");
        safe_exit(1);
    } else if (result == 0) {
        perror("invalid ip address in \"inet_pton\"");
        safe_exit(1);
    }
    server_addr = (struct sockaddr*) &server;
    socket_des = make_socket();
    connected = 1;
    int numTries = 0;
    while (socket_des == -1 && numTries < NUMTRIES)
    {
        connected = 0;
        numTries++;
        printf("Connecting (attempt #%d)...\n", numTries);
        connected = 1;
        socket_des = make_socket();
    }
    if (numTries == NUMTRIES)
    {
        connected = 0;
        printf("Could not connect to server\n");
        printf("Closing program\n");
        safe_exit(1);
    }
    else
    {
        printf("Connection successful\n");
    }
    
    int numsubdirs;
    // Process existing files on startup
    if ((numsubdirs = processdir(argv[1], &socket_des, 0)) < 0)
    {
        // The function already prints the appropriate error message
        safe_exit(1);
    }
    printf("Finished processing existing directories\n");
    
    // Look for file/directory additions
    int fd = inotify_init();
    if (fd < 0)
    {
        perror("inotify_init");
        safe_exit(1);
    }
    
    // Create array that stores info for watching subdirs
    watched_entry_t subdirstowatch[numsubdirs];
    char names[numsubdirs][256];
    
    DIR* directory = opendir(argv[1]);
    struct dirent* subdir;
    struct stat pathStats;
    int i = 0;
    int arrsize;
    char fullpath[256];
    while ((subdir = readdir(directory)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            strcpy(fullpath, argv[1]);
            strcat(fullpath, "/");
            strcat(fullpath, subdir->d_name);
            stat(fullpath, &pathStats);
            if (S_ISDIR(pathStats.st_mode))
            {
                subdirstowatch[i].fd = inotify_add_watch(fd, fullpath, IN_CLOSE_WRITE);
                strcpy(names[i], fullpath);
                subdirstowatch[i].path = names[i];
                i++;
            }
        }
    }
    int watchinginitdirs = 1;
    
    // This watch will notice any new files or subdirectories in the directory we are watching
    int rootdirfd = inotify_add_watch(fd, argv[1], IN_CREATE | IN_CLOSE_WRITE);
    
    char buffer[EVENT_BUF_LEN];
    char ndirname[256];

    struct timeval timeout;
    int rlen;

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
            result = 0;
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
                        safe_exit(1);
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
                        result = send_until_success(&socket_des, fullname, 0);
                    }
                    closedir(basedir);
                    
                    if (watchinginitdirs)
                    {
                        i = 0;
                        while (i < numsubdirs)
                        {
                            inotify_rm_watch(fd, subdirstowatch[i].fd);
                            i++;
                        }
                        watchinginitdirs = 0;
                    }
                    
                }
                /* Check for a new file */
                else if (((IN_CLOSE_WRITE) & ev->mask) && !(IN_ISDIR & ev->mask))
                {
                    if (watchinginitdirs && child.fd != ev->wd && rootdirfd != ev->wd)
                    {
                        int index = 0;
                        int index2 = 0;
                        while (index < numsubdirs)
                        {
                            if (subdirstowatch[index].fd == ev->wd)
                            {
                                child.fd = ev->wd;
                                strcpy(ndirname, subdirstowatch[index].path);
                                child.path = ndirname;
                                break;
                            }
                            index++;
                        }
                        if (index == numsubdirs)
                        {
                            fprintf(stderr, "Error, got unexpected file close FD\n");
                        }
                        else
                        {
                            while (index2 < numsubdirs)
                            {
                                if (index != index2)
                                {
                                    inotify_rm_watch(fd, subdirstowatch[index2].fd);
                                }
                                index2++;
                            }
                            watchinginitdirs = 0;
                        }
                    }
                    if (child.fd == ev->wd || rootdirfd == ev->wd)
                    {
                        char* parentdir;
                        int inRootDir;
                        if (child.fd == ev->wd)
                        {
                            parentdir = child.path;
                            inRootDir = 0;
                        }
                        else
                        {
                            parentdir = argv[1];
                            inRootDir = 1;
                        }
                        char fullname [256];
                        strcpy(fullname, parentdir);
                        strcat(fullname,"/");
                        strcat(fullname, ev->name);
                        result = send_until_success(&socket_des, fullname, inRootDir);
                    }
                }
                if (result == 1)
                {
                    printf("Could not read %s (file not sent)\n", ev->name);
                }
                else if (result == -1)
                {
                    printf("Connection to server was lost, could not reconnect\n");
                    printf("Closing program\n");
                    safe_exit(1);
                }
            }
            i += EVENT_SIZE + ev->len;
        }
    }
}
