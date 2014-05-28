#define EVENT_BUF_LEN 128
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define ADDRESSD "127.0.0.1" // for bw.cal-sdb.org, this should be "54.241.13.58"
#define ADDRESSP 1883

#include <errno.h>
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

uint32_t sendid = 0;

int send_file(int socket_descriptor, const char* filepath, int inRootDir)
{
    printf("Processing %s\n", filepath);
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

int processdir(const char* dirpath, int socket_descriptor, int depth)
{
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
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                send_file(socket_descriptor, fullpath, depth == 0);
            }
        }
    }
    if (errno != 0) {
        printf("Could not finish reading directory %s\n", dirpath);
        return -1;
    }
    return 0;
}

int make_socket(struct sockaddr* server_addr)
{
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
    return socket_descriptor;
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
    int socket_des = make_socket((struct sockaddr*) &server);
    
    // Process existing files on startup
    if (processdir(argv[1], socket_des, 0) < 0)
    {
        // The function already prints the appropriate error message
        exit(1);
    }
    printf("Finished processing existing directories\n");
    
    // Look for file/directory additions
    int fd = inotify_init();
    if (fd < 0)
    {
        perror("inotify_init");
        exit(1);
    }
    
    // This watch will notice any new files or subdirectories in the directory we are watching
    int rootdirfd = inotify_add_watch(fd, argv[1], IN_CLOSE_WRITE | IN_CREATE);
    
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
                        send_file(socket_des, fullname, 0);
                    }
                    closedir(basedir);
                }
                /* Check for a new file */
                else if (((IN_CLOSE_WRITE) & ev->mask) && !(IN_ISDIR & ev->mask))
                {
                    if (child.fd == ev->wd || rootdirfd == ev->wd)
                    {
                        char* parentdir;
                        if (child.fd == ev->wd)
                        {
                            parentdir = child.path;
                        }
                        else
                        {
                            parentdir = argv[1];
                        }
                        char fullname [256];
                        printf("File closed after writing: %s/%s\n", parentdir, ev->name);
                        strcpy(fullname, parentdir);
                        strcat(fullname,"/");
                        strcat(fullname, ev->name);
                        printf("Processing\n");
                        send_file(socket_des, fullname, 0);
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
