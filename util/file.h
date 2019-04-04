#pragma once
#ifdef OS_WIN
#include "fileOptWindows.h"
#elif defined OS_LINUX
#include "fileOptUnix.h"
#endif 
