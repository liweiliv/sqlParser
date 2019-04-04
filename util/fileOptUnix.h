/*
 * fileOpt.h
 *
 *  Created on: 2019年1月23日
 *      Author: liwei
 */

#ifndef FILEOPT_H_
#define FILEOPT_H_
#ifdef OS_LINUX
#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#define fileHandle int
static fileHandle openFile(const char *file, bool read, bool write, bool create)
{
	int fd = 0;
	int flag = 0;
	if (read)
		flag |= O_RDONLY;
	if (write)
		flag |= O_WRONLY;
	if (create)
		flag |= O_CREAT;
	return (fd = open(file, flag, create ? S_IRUSR | S_IWUSR | S_IRGRP : 0));
}
static inline int64_t readFile(fileHandle fd, char *buf, uint64_t count)
{
    uint64_t readbytes, save_count=0;
    for (;;)
    {
        errno= 0;
        readbytes = read(fd, buf+save_count, count-save_count);
        if (readbytes != count-save_count)
        {
            if ((readbytes == 0 || (int) readbytes == -1)&&errno == EINTR)
                continue; /* Interrupted */

            if (readbytes == (size_t) -1)
                return -errno; /* Return with error */
            else if(readbytes==0)
                return save_count;
            else
                save_count += readbytes;
        }
        else
        {
            save_count += readbytes;
            break;
        }
    }
    return save_count;
}
static inline  int64_t writeFile(fileHandle fd,const char *buf, size_t count)
{
    uint64_t writebytes, save_count=0;
    for (;;)
    {
        errno= 0;
        writebytes = write(fd, buf+save_count, count-save_count);
        if (writebytes != count-save_count)
        {
            if ((writebytes == 0 || (int) writebytes == -1)&&errno == EINTR)
                continue; /* Interrupted */
            if (writebytes == (size_t) -1)
                return -errno; /* Return with error */
            else if(writebytes==0)
                return save_count;
            else
                save_count += writebytes;
        }
        else
        {
            save_count += writebytes;
            break;
        }
    }
    return save_count;
}
static int truncateFile(fileHandle fd, uint64_t offset)
{
	return ftruncate(fd, offset);
}
static int closeFile(fileHandle fd)
{
	return close(fd);
}
static int64_t seekFile(fileHandle fd, int64_t position, int seekType)
{
    return lseek64(fd,position,seekType);
}
/*
 * -1 文件存在
 * 0 文件不存在
 * 其他为错误号
 */
static inline  int checkFileExist(const char *filename)
{
	fileHandle fd=open(filename,O_RDONLY);
   if(fd>0)
   {
       close(fd);
       return -1;
   }
   else
   {
       if(errno==ENOENT)
       {
           return 0;
       }
       else
       {
           return errno;
       }
   }
}
#endif
#endif /* FILEOPT_H_ */
