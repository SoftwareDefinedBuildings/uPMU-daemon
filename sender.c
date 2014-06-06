#define EVENT_BUF_LEN 128 * ( sizeof (struct inotify_event) )
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define ADDRESSP 1883
#define FULLPATHLEN 64 // the maximum length of a full file path
#define FILENAMELEN 28 // the maximum length of a file or directory name (within the root directory)
#define TIMEDELAY 10 // the number of seconds to wait between subsequent tries to reconnect

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
    char* path;
} watched_entry_t;

typedef struct
{
    long filenum;
    char filename[FILENAMELEN];
} file_entry_t;

watched_entry_t child;

// Three pointers that will point to malloc'ed arrays, and ints that indicate if they need to be freed
file_entry_t* filearr;
int filearractive = 0;

file_entry_t* subdirarr;
int subdirarractive = 0;

file_entry_t* subdirfiles;
int subdirfilesactive = 0;

fd_set set;

// information about the root directory
int rootdirOpen = 0; // 1 if open (i.e., root directory needs to be closed)
DIR* rootdir;
char rootpath[FULLPATHLEN];
int numfiles;
int numsubdirs;
off_t start_position; // lets us "seek" the start without updating the directory

// when processing directories upon initialization, store the watched directories in an array
int num_watched_dirs = 0;
int size_watched_arr = 0;

// the socket descriptor
int socket_des = -1;

// 1 if connected (i.e., socket connection needs to be closed), 0 otherwise
int connected = 0;

// pointer to the server address
struct sockaddr* server_addr;

uint32_t sendid = 1;

/* Close the socket connection. */
void close_connection(int socket_descriptor)
{
    shutdown(socket_descriptor, SHUT_RDWR);
    close(socket_descriptor);
}

/* Exit, closing the socket connection if necessary. */
void safe_exit(int arg)
{
    if (rootdirOpen)
    {
        closedir(rootdir);
    }
    if (connected)
    {
        close_connection(socket_des);
    }
    if (filearractive)
    {
        free(filearr);
    }
    if (subdirarractive)
    {
        free(subdirarr);
    }
    if(subdirfilesactive)
    {
        free(subdirfiles);
    }
    exit(arg);
}

int send_until_success(int* socket_descriptor, const char* filepath, int inRootDir)
{
    int result;
    while ((result = send_file(*socket_descriptor, filepath, inRootDir)) == -1)
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
    char filepathAfterParent[FULLPATHLEN];
    char filepathCopy[FULLPATHLEN];
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
    *((uint32_t*) data) = sendid;
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

int file_entry_comparator(const void* f1, const void* f2)
{
    const file_entry_t* fe1 = (const file_entry_t*) f1;
    const file_entry_t* fe2 = (const file_entry_t*) f2;
    if ((fe1->filenum) < (fe2->filenum))
    {
        return -1;
    }
    else if ((fe1->filenum) > (fe2->filenum))
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

long parse_filename(const char* filename)
{
    const char* currchar = filename;
    while (*currchar != '\0' && (*currchar < '0' || *currchar > '9'))
    {
        currchar++;
    }
    if (*currchar == '\0')
    {
        printf("Warning: file %s could not be ordered numerically\n", filename);
        return -1;
    }
    errno = 0;
    long result = strtol(currchar, NULL, 10);
    if (errno != 0)
    {
        printf("Type long is not large enough to store file number for %s\n", filename);
        printf("Files may be sent out of order\n");
    }
    return result;
}

/* Updates global variables rootdir, numfiles, numsubdirs, start_position, rootpath */
int updaterootdir(const char* dirpath)
{
    if (strlen(dirpath) >= FULLPATHLEN - 5)
    {
        printf("%s too large: all filepaths must be less than %d characters long\n", dirpath, FULLPATHLEN);
        return -1;
    }
    strcpy(rootpath, dirpath);
    if (rootpath[strlen(rootpath) - 1] != '/')
    {
        strcat(rootpath, "/");
    }
    rootdir = opendir(rootpath);
    if (rootdir == NULL)
    {
        printf("%s is not a valid directory\n", rootpath);
        return -1;
    }
    struct dirent* subdir;
    struct stat pathStats;
    start_position = telldir(rootdir);
    numsubdirs = 0;
    numfiles = 0;
    // Count the number of files and subdirectories in the dirpath
    int pathlen;
    char fullpath[FULLPATHLEN];
    while ((subdir = readdir(rootdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            pathlen = strlen(rootpath) + strlen(subdir->d_name) + 1;
            if (pathlen > FULLPATHLEN)
            {
                printf("Path length of %d found; max path length is %d\n", pathlen, FULLPATHLEN);
                return -1;
            }
            strcpy(fullpath, rootpath);
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
    return 0;
}

/* Processes root directory, sending files and adding watches (uses information in global variables) */
int processrootdir(int* socket_descriptor, char subdirnames[][FILENAMELEN], watched_entry_t* subdirinfo, int inotify_fd, int* rootdir_fd)
{
    struct dirent* subdir;
    struct stat pathStats;
    filearr = malloc(numfiles * sizeof(file_entry_t));
    filearractive = 1;
    unsigned int fileIndex = 0;
    subdirarr = malloc(numsubdirs * sizeof(file_entry_t));
    subdirarractive = 1;
    unsigned int subdirIndex = 0;
    // Add watch for root directory
    *rootdir_fd = inotify_add_watch(inotify_fd, rootpath, IN_CREATE | IN_CLOSE_WRITE);
    // Add files and directories to arrays
    int pathlen;
    char fullpath[FULLPATHLEN];
    while ((subdir = readdir(rootdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            pathlen = strlen(rootpath) + strlen(subdir->d_name) + 2;
            if (pathlen > FULLPATHLEN)
            {
                printf("Filepath of length %d found; max allowed is %d\n", pathlen, FULLPATHLEN);
                return -1;
            }
            strcpy(fullpath, rootpath);
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
                strcpy(subdirarr[subdirIndex].filename, subdir->d_name);
                strcat(subdirarr[subdirIndex].filename, "/");
                subdirarr[subdirIndex].filenum = parse_filename(subdir->d_name);
                subdirIndex++;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                if (strlen(subdir->d_name) >= FILENAMELEN)
                {
                    printf("File %s has name length of %d; max allowed is %d\n", subdir->d_name, (int) strlen(subdir->d_name), FILENAMELEN - 1);
                    return -1;
                }
                strcpy(filearr[fileIndex].filename, subdir->d_name);
                filearr[fileIndex].filenum = parse_filename(subdir->d_name);
                fileIndex++;
            }
        }
    }
    if (fileIndex != numfiles || subdirIndex != numsubdirs)
    {
        printf("Error: inconsistent file count in root directory (possibly because files were added while counting)\n");
        return -1;
    }
    
    // Sort files and subdirectories in numerical order
    qsort(filearr, numfiles, sizeof(file_entry_t), file_entry_comparator);
    qsort(subdirarr, numsubdirs, sizeof(file_entry_t), file_entry_comparator);
    
    for (fileIndex = 0; fileIndex < numfiles; fileIndex++)
    {
        strcpy(fullpath, rootpath);
        strcat(fullpath, filearr[fileIndex].filename);
        send_until_success(socket_descriptor, fullpath, 1);
    }
    
    filearractive = 0;
    free(filearr);
    
    DIR* subdirtoprocess;
    int result;
    // Process directories, adding watches
    for (subdirIndex = 0; subdirIndex < numsubdirs; subdirIndex++)
    {
        strcpy(fullpath, rootpath);
        strcat(fullpath, subdirarr[subdirIndex].filename);
        subdirtoprocess = opendir(fullpath);
        // Add watch, keep track of name
        strcpy(subdirnames[subdirIndex], subdirarr[subdirIndex].filename);
        subdirinfo[subdirIndex].path = subdirnames[subdirIndex];
        subdirinfo[subdirIndex].fd = inotify_add_watch(inotify_fd, fullpath, IN_CLOSE_WRITE);
        // Process subdirectories
        result = processsubdir(socket_descriptor, subdirtoprocess, fullpath);
        closedir(subdirtoprocess);
        if (result < 0)
        {
            return -1;
        }
    }
    
    subdirarractive = 0;
    free(subdirarr);
    
    printf("Finished processing existing files.\n");
    return 0;
}

int processsubdir(int* socket_descriptor, DIR* processingdir, char* dirpath)
{
    struct dirent* subdir;
    struct stat pathStats;
    
    off_t start = telldir(processingdir);
    int numfiles = 0;

    int pathlen;
    char fullpath[FULLPATHLEN];
    while ((subdir = readdir(processingdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            pathlen = strlen(dirpath) + strlen(subdir->d_name) + 1;
            if (pathlen > FULLPATHLEN)
            {
                printf("Filepath of length %d found; max allowed is %d\n", pathlen, FULLPATHLEN);
                return -1;
            }
            strcpy(fullpath, dirpath);
            strcat(fullpath, subdir->d_name);
            if (stat(fullpath, &pathStats) != 0)
            {
                printf("Could not read file %s\n", fullpath);
                continue;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                numfiles++;
            }
        }
    }
    subdirfiles = malloc(numfiles * sizeof(file_entry_t));
    subdirfilesactive = 1;
    seekdir(processingdir, start);
    int fileIndex = 0;
    while ((subdir = readdir(processingdir)) != NULL)
    {
        if (strcmp(subdir->d_name, ".") != 0 && strcmp(subdir->d_name, "..") != 0)
        {
            // We just checked the filepath lengths in the previous loop, we won't do it gain
            strcpy(fullpath, dirpath);
            strcat(fullpath, subdir->d_name);
            if (stat(fullpath, &pathStats) != 0)
            {
                printf("Could not read file %s\n", fullpath);
                continue;
            }
            else if (S_ISREG(pathStats.st_mode))
            {
                strcpy(subdirfiles[fileIndex].filename, subdir->d_name);
                subdirfiles[fileIndex].filenum = parse_filename(subdir->d_name);
                fileIndex++;
            }
        }
    }
    if (fileIndex != numfiles)
    {
        printf("Error: inconsistent file count in root directory (possibly because files were added while counting)\n");
        return -1;
    }
    
    qsort(subdirfiles, numfiles, sizeof(file_entry_t), file_entry_comparator);
    
    for (fileIndex = 0; fileIndex < numfiles; fileIndex++)
    {
        strcpy(fullpath, dirpath);
        strcat(fullpath, subdirfiles[fileIndex].filename);
        if (send_until_success(socket_descriptor, fullpath, 0) < 0)
        {
            printf("Could not read %s (file not sent)\n", fullpath);
        }
    }
    subdirfilesactive = 0;
    free(subdirfiles);
    
    return 0;
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
        printf("Usage: %s <directorytowatch> <targetserver>\n", argv[0]);
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
    
    if (updaterootdir(argv[1]) < 0)
    {
        safe_exit(1);
    }
    
    // Create arrays that store info for watching subdirs
    watched_entry_t subdirstowatch[numsubdirs];
    char names[numsubdirs][FILENAMELEN]; // To save memory, only stores the name of the directory, not the path to it
    
    // This watch will notice any new files or subdirectories in the directory we are watching
    int rootdirfd;
    if (processrootdir(&socket_des, names, subdirstowatch, fd, &rootdirfd) < 0)
    {
        safe_exit(1);
    }
    int watchinginitdirs = 1;
    closedir(rootdir);
    
    char buffer[EVENT_BUF_LEN];
    char ndirname[FILENAMELEN];

    struct timeval timeout;
    int rlen;

    child.fd = -1;
    int i;
    
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
                    /* Check for new files within the directory */
                    strncpy(ndirname, ev->name, sizeof(ndirname)-1);
                    strncat(ndirname, "/", sizeof(ndirname) - 1);
                    strcpy(fullname, rootpath);
                    strcat(fullname, ndirname);
                    watched_entry_t et = {.fd = inotify_add_watch(fd, fullname, IN_CLOSE_WRITE),
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
                        strcpy(fullname, rootpath);
                        strcat(fullname, ndirname);
                        strcat(fullname, dir->d_name);
                        if (send_until_success(&socket_des, fullname, 0) == 1)
                        {
                             printf("Could not read %s (file not sent)\n", fullname);
                        }
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
                            parentdir = "";
                            inRootDir = 1;
                        }
                        char fullname[FULLPATHLEN];
                        strcpy(fullname, rootpath);
                        strcat(fullname, parentdir);
                        strcat(fullname, ev->name);
                        result = send_until_success(&socket_des, fullname, inRootDir);
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
