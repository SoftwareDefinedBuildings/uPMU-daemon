#define EVENT_BUF_LEN 128 * ( sizeof (struct inotify_event) )
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define ADDRESSP 1883
#define FULLPATHLEN 256 // the maximum length of a full file path
#define FILENAMELEN 128 // the maximum length of a file or directory name (within the root directory)
#define TIMEDELAY 10 // the number of seconds to wait between subsequent tries to reconnect
#define MAXDEPTH 4 // the root directory is at depth 0

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

/* When my comments refer to the "root directory", they mean the directory the program is watching */

typedef struct 
{
    int fd;
    char path[FULLPATHLEN];
} watched_entry_t;

watched_entry_t children[MAXDEPTH + 1];

fd_set set;

// information about the root directory
int rootdirOpen = 0; // 1 if open (i.e., root directory needs to be closed)
char rootpath[FULLPATHLEN];

// when processing directories upon initialization, store the watched directories in an array
int num_watched_dirs = 0;
int size_watched_arr = 0;

// the socket descriptor
int socket_des = -1;

// the serial number of this uPMU
char* serialNum;
uint32_t size_serial;
uint32_t size_serial_word;

// 1 if connected (i.e., socket connection needs to be closed), 0 otherwise
int connected = 0;

// pointer to the server address
struct sockaddr* server_addr;

// the id of the next message sent to the server
uint32_t sendid = 1;

/* Deletes a directory if possible, printing messages as necessary. */
void remove_dir(const char* dirpath)
{
    errno = 0;
    if (rmdir(dirpath) == 0)
    {
        printf("Successfully removed directory %s\n", dirpath);
    }
    else
    {
        if (errno == ENOTEMPTY || errno == EEXIST)
        {
            printf("Directory %s not removed: still contains files\n", dirpath);
        }
        else
        {
            printf("Directory %s not removed: no permissions OR directory in use\n", dirpath);
        }
    }
}

/* Finds the smallest int larger than the input that's a multiple of 4. */
uint32_t roundUp4(uint32_t input)
{
    return (input + 3) & 0xFFFFFFFCu;
}

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

/* Attempts to connect to the server_addr and returns the socket descriptor
 * if successful. If not successful, return -1.
 */
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

/* Sends the contents of the file at FILEPATH over TCP using SOCKET_DESCRIPTOR, if it
 * is a .dat file.
 * The total data sent is: 1. an id number, 2. the length of the filepath, 3.
 * the filepath, 4. the length of the contents of the file, and 5. the contents
 * of the file.
 * The filepath sent includes the filename itself, and contains the parent directory
 * if INROOTDIR is 1.
 * Returns 0 if the transmission was successful or if the file did not have to be sent.
 * Returns 1 if there was an error reading the file.
 * Returns -1 if the file was read properly but could not be sent.
 */
int send_file(int socket_descriptor, const char* filepath)
{
    printf("Sending file %s\n", filepath);
    
    // Ignore files that are not .dat
    const char* substr = filepath + strlen(filepath) - 4;
    if (strcmp(substr, ".dat") != 0)
    {
        printf("Skipping over file that is not \".dat\"\n");
        return 0;
    }

    uint32_t size = strlen(filepath);
    
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
    
    // Store file number (sendid), length of serial number, length of filename, length of data, and filename in data array
    // The null terminator may be overwritten; the length of the filename does not
    // include the null terminator.
    // Space is added to the end of filename so it is word-aligned.
    uint32_t size_word = roundUp4(size); // size with extra bytes so it's word-aligned
    uint8_t data[16 + size_word + size_serial_word + length] __attribute__((aligned(4)));
    *((uint32_t*) data) = sendid;
    *((uint32_t*) (data + 4)) = size;
    *((uint32_t*) (data + 8)) = size_serial;
    *((uint32_t*) (data + 12)) = length;
    strcpy(data + 16, filepath);
    strcpy(data + 16 + size_word, serialNum);
    uint8_t* datastart = data + 16 + size_word + size_serial_word;
    rewind(input);
    
    // Read data into array
    int32_t dataread = fread(datastart, 1, length, input);
    fclose(input);
    if (dataread != length) {
        printf("Error: could not finish reading file (read %d out of %d bytes)\n", dataread, length);
        return 1;
    }
    
    // Send message over TCP
    int dataleft = 16 + size_word + size_serial_word + length;
    int datawritten;
    uint8_t* dest = data;
    while (dataleft > 0)
    {
        datawritten = write(socket_descriptor, dest, dataleft);
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
        printf("Received improper confirmation of receipt of %s (will not be deleted)\n", filepath);
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
    if (sendid == 0xFFFFFFFFu)
    {
        sendid = 1;
    }
    return 0;
}

/* Same as send_file(), except that it takes a pointer to the socket_descriptor instead
 * of the value itself, and that it will repeatedly try to send the file (waiting
 * TIMEDELAY seconds between attempts) if the file could be read but could not be sent
 * over TCP. It returns the value of the successful attempt (so it will never return -1,
 * just 0 or 1).
 */
int send_until_success(int* socket_descriptor, const char* filepath)
{
    int result;
    while ((result = send_file(*socket_descriptor, filepath)) == -1)
    {
        if (connected)
        {
            close_connection(*socket_descriptor);
            connected = 0;
        }
        sleep(TIMEDELAY);
        printf("Attempting to reconnect...\n");
        *socket_descriptor = make_socket();
    }
    connected = 1;
    return result;
}

/* Used to compare two file entries so they can be sorted. */
int file_entry_comparator(const void* f1, const void* f2)
{
    return strcmp((const char*) f1, (const char*) f2);
}

/* Processes directory, sending files and adding watches (uses information in global variables) */
int processdir(const char* dirpath, int* socket_descriptor, int inotify_fd, int depth, int addwatchtosubs)
{
    if (strlen(dirpath) >= FULLPATHLEN - 5)
    {
        printf("%s too large: all filepaths must be less than %d characters long\n", dirpath, FULLPATHLEN);
        return -1;
    }
    DIR* rootdir = opendir(dirpath);
    if (rootdir == NULL)
    {
        printf("%s is not a valid directory\n", dirpath);
        return -1;
    }
    struct dirent* subdir;
    struct stat pathStats;
    off_t start_position = telldir(rootdir);
    int numsubdirs = 0;
    int numfiles = 0;
    // Count the number of files and subdirectories in the dirpath
    int pathlen;
    char fullpath[FULLPATHLEN];
    while ((subdir = readdir(rootdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            pathlen = strlen(dirpath) + strlen(subdir->d_name) + 1;
            if (pathlen > FULLPATHLEN)
            {
                printf("Path length of %d found; max path length is %d\n", pathlen, FULLPATHLEN);
                return -1;
            }
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
                numsubdirs++;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                numfiles++;
            }
        }
    }
    
    seekdir(rootdir, start_position);
    char* filearr = malloc(numfiles * FILENAMELEN);
    unsigned int fileIndex = 0;
    char* subdirarr = malloc(numsubdirs * FILENAMELEN);
    unsigned int subdirIndex = 0;
    // Add watch for root directory
    // Add files and directories to arrays
    while ((subdir = readdir(rootdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            pathlen = strlen(dirpath) + strlen(subdir->d_name) + 2;
            if (pathlen > FULLPATHLEN)
            {
                printf("Filepath of length %d found; max allowed is %d\n", pathlen, FULLPATHLEN);
                return -1;
            }
            strcpy(fullpath, dirpath);
            strcat(fullpath, "/");
            strcat(fullpath, subdir->d_name);
            if (stat(fullpath, &pathStats) != 0)
            {
                printf("Could not read file %s\n", fullpath);
                continue;
            }
            if (S_ISDIR(pathStats.st_mode))
            {
                if (strlen(subdir->d_name) >= FILENAMELEN - 1)
                {
                    printf("Directory %s has name length of %d; max allowed is %d\n", subdir->d_name, (int) strlen(subdir->d_name), FILENAMELEN - 2);
                    return -1;
                }
                strcpy(subdirarr + (subdirIndex * FILENAMELEN), subdir->d_name);
                strcat(subdirarr + (subdirIndex * FILENAMELEN), "/");
                subdirIndex++;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                if (strlen(subdir->d_name) >= FILENAMELEN)
                {
                    printf("File %s has name length of %d; max allowed is %d\n", subdir->d_name, (int) strlen(subdir->d_name), FILENAMELEN - 1);
                    return -1;
                }
                strcpy(filearr + (fileIndex * FILENAMELEN), subdir->d_name);
                fileIndex++;
            }
        }
    }
    if (fileIndex != numfiles || subdirIndex != numsubdirs)
    {
        printf("Error: inconsistent file count in root directory (possibly because files were added while counting)\n");
        return -1;
    }
    
    closedir(rootdir);
    // Sort files and subdirectories in numerical order
    qsort(filearr, numfiles, FILENAMELEN, file_entry_comparator);
    qsort(subdirarr, numsubdirs, FILENAMELEN, file_entry_comparator);
    
    for (fileIndex = 0; fileIndex < numfiles; fileIndex++)
    {
        strcpy(fullpath, dirpath);
        strcat(fullpath, filearr + (fileIndex * FILENAMELEN));
        send_until_success(socket_descriptor, fullpath);
    }
    
    free(filearr);
    
    int result;
    int addedwatch;
    // Process directories, adding watches
    for (subdirIndex = 0; subdirIndex < numsubdirs; subdirIndex++)
    {
        addedwatch = 0;
        strcpy(fullpath, dirpath);
        strcat(fullpath, subdirarr + (subdirIndex * FILENAMELEN));
        if (addwatchtosubs && (subdirIndex == (numsubdirs - 1)))
        {
            children[depth].fd = inotify_add_watch(inotify_fd, fullpath, IN_CREATE | IN_CLOSE_WRITE);
            strcpy(children[depth].path, fullpath);
            addedwatch = 1;
        }
        result = processdir(fullpath, socket_descriptor, inotify_fd, depth + 1, addwatchtosubs && (subdirIndex == (numsubdirs - 1)));
        if (result < 0)
        {
            return -1;
        }
        if (!addedwatch)
        {
            remove_dir(fullpath);
        }
    }
    free(subdirarr);
    
    if (depth == 1)
    {
        printf("Finished processing existing files.\n");
    }
    return 0;
}

void interrupt_handler(int sig)
{
    safe_exit(0);
}

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        printf("Usage: %s <directorytowatch> <targetserver> <uPMU serial number>\n", argv[0]);
        safe_exit(1);
    }
    
    serialNum = argv[3];
    size_serial = strlen(serialNum);
    size_serial_word = roundUp4(size_serial);
    
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
    connected = 0;
    printf("Attempting to connect...\n");
    socket_des = make_socket();
    while (socket_des == -1)
    {
        sleep(TIMEDELAY);
        printf("Attempting to connect...\n");
        socket_des = make_socket();
    }
    connected = 1;
    printf("Connection successful\n");
    
    // Look for file/directory additions
    int fd = inotify_init();
    if (fd < 0)
    {
        perror("inotify_init");
        safe_exit(1);
    }
    
    // This watch will notice any new files or subdirectories in the directory we are watching
    children[0].fd = inotify_add_watch(fd, argv[1], IN_CREATE | IN_CLOSE_WRITE);
    strcpy(children[0].path, argv[1]);
    if (argv[1][strlen(argv[1]) - 1] != '/')
    {
        strcat(children[0].path, "/");
    }
    if (processdir(children[0].path, &socket_des, fd, 1, 1) < 0)
    {
        safe_exit(1);
    }
    
    char buffer[EVENT_BUF_LEN];

    struct timeval timeout;
    int rlen;

    int i, j;
    
    char fullname[FULLPATHLEN];

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
        if (rlen == -1)
        {
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
                    printf("new directory noticed\n");
                    // Find the correct depth, store it in i
                    for (i = MAXDEPTH; i >= 0; i--)
                    {
                        if (children[i].fd == ev->wd)
                        {
                            break;
                        }
                    }
                    i++;
                    if (i > MAXDEPTH)
                    {
                        printf("WARNING: unexpected new directory past maximum depth (will be ignored)\n");
                    }
                    
                    // remove watches from depth i and deeper and delete directories if possible
                    printf("Found new directory %s at depth %d\n", children[i].path, i);
                    for (j = MAXDEPTH; j >= i; j--)
                    {
                        if (children[j].fd != -1) // if it has already been deleted, don't do anything
                        {
                            if (inotify_rm_watch(fd, children[j].fd))
                            {
                                perror("RM watch");
                            }
                            else
                            {
                                children[j].fd = -1; // mark it as deleted
                            }
                            remove_dir(children[j].path);
                        }
                    }
                    
                    char* parentname = children[i - 1].path;
                    strcpy(fullname, parentname);
                    strcat(fullname, ev->name);
                    strcat(fullname, "/");
                    strcpy(children[i].path, fullname);
                    printf("New path: %s\n", fullname);
                    children[i].fd = inotify_add_watch(fd, children[i].path, IN_CREATE | IN_CLOSE_WRITE);
                    
                    // Ok great, but we may have missed some files, so let's check for them:
                    DIR *basedir;
                    struct dirent *dir;
                    basedir = opendir(fullname);
                    if (!basedir)
                    {
                        perror("basedir");
                        printf("Tried to open %s\n", fullname);
                        safe_exit(1);
                    }
                    while ((dir = readdir(basedir)) != NULL)
                    {
                        if (dir->d_name[0] == '.')
                            continue;
                        printf("Checking out appeared file\n");
                        strcpy(fullname, children[i].path);
                        strcat(fullname, "/");
                        strcat(fullname, dir->d_name);
                        if (send_until_success(&socket_des, fullname) == 1)
                        {
                             printf("Could not read %s (file not sent)\n", fullname);
                        }
                    }
                    closedir(basedir);
                }
                /* Check for a new file */
                else if (((IN_CLOSE_WRITE) & ev->mask) && !(IN_ISDIR & ev->mask))
                {
                    if (children[MAXDEPTH].fd == ev->wd)
                    {
                        char fullname[FULLPATHLEN];
                        strcpy(fullname, children[MAXDEPTH].path);
                        strcat(fullname, ev->name);
                        result = send_until_success(&socket_des, fullname);
                    }
                    else
                    {
                        printf("Warning: file %s appeared outside hour directory\n", ev->name);
                    }
                }
                if (result == 1)
                {
                    printf("Could not read %s (file already sent or deleted concurrently)\n", ev->name);
                }
            }
            i += EVENT_SIZE + ev->len;
        }
    }
}
