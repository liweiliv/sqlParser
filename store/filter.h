/*
 * filter.h
 *
 *  Created on: 2019年1月8日
 *      Author: liwei
 */

#ifndef FILTER_H_
#define FILTER_H_
#include "metaDataCollection.h"
#include "record.h"
namespace STORE{
    struct filter{
        unsigned char * filters;
        unsigned int size;
        filter(){

        }
        int init(const char *filters)
        {

        }
        virtual bool doFilter(const char * data){

        }
        bool continer(const char *filterString)
        {

        }
    };
}




#endif /* FILTER_H_ */
