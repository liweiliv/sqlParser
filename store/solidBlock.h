/*
 * solidBlock.h
 *
 *  Created on: 2019年1月7日
 *      Author: liwei
 */
#ifndef SOLIDBLOCK_H_
#define SOLIDBLOCK_H_
#include "index.h"
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include "iterator.h"
namespace STORE
{
class solidBlock {
    void * a(){
        void * a = __null;
    }

};
class solidBlockIterator :public iterator{

}
}

#endif
