#pragma once
#include <windows.h>
#include <stdint.h>
#define fileHandle HANDLE 

static fileHandle openFile(const char *file,bool read,bool write,bool create)
{
	uint64_t flag = 0;
	if (read)
		flag |= GENERIC_READ;
	if (write)
		flag |= GENERIC_WRITE;
	fileHandle fd = CreateFile(file, flag, 0, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (fd == INVALID_HANDLE_VALUE)
	{
		uint64_t errCode = GetLastError();
		if (errCode == ERROR_FILE_NOT_FOUND)//no such file
		{
			if (create)
			{
				fd = CreateFile(file, flag, 0, nullptr, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
				return fd;
			}
		}
		return INVALID_HANDLE_VALUE;
	}
	else
		return fd;
}
static int64_t seekFile(fileHandle fd, int64_t position, int seekType)
{
	LARGE_INTEGER li;
	li.QuadPart = position;
	li.LowPart = SetFilePointer(fd,li.LowPart,&li.HighPart,seekType);
	if (li.LowPart == INVALID_SET_FILE_POINTER && GetLastError()
		!= NO_ERROR)
	{
		li.QuadPart = -1;
	}
	return li.QuadPart;
}
static int truncateFile(fileHandle fd, uint64_t offset)
{
	if (offset != seekFile(fd, offset, SEEK_SET))
		return -1;
	return SetEndOfFile(fd);
}
static int64_t writeFile(fileHandle fd, const char* data, uint64_t size)
{
	DWORD writed = 0;
	uint64_t remain = size;
	while (!WriteFile(fd, data+(size- remain), size, &writed, nullptr))
	{
		if (writed > 0)
		{
			remain -= writed;
			writed = 0;
		}
		else
		{
			if (remain == size)//write no thing
				return -1;
			else//rollback
			{
				if (seekFile(fd, remain - size, SEEK_CUR) < 0)
				{
					return -2;
				}
			}
		}
	}
	return size - remain;
}
static int64_t readFile(fileHandle fd, char *buf, uint64_t size)
{
	DWORD readed = 0;
	uint64_t remain = size;
	while (!ReadFile(fd, buf+(size-remain), size, &readed, nullptr))
	{
		if (readed > 0)
		{
			remain -= readed;
			readed = 0;
		}
		else
		{
			if (remain == size)//read no thing
				return -1;
			else//rollback
			{
				if (seekFile(fd, remain - size, SEEK_CUR) < 0)
				{
					return -2;
				}
			}
		}
	}
	return size - remain;
}
static int closeFile(fileHandle fd)
{
	return CloseHandle(fd);
}
static int fsync(fileHandle fd)
{
	return FlushFileBuffers(fd);
}
