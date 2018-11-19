/*
 * SQLStringUtil.cpp
 *
 *  Created on: 2018年11月17日
 *      Author: liwei
 */
#include "trieTree.h"
#include "SQLStringUtil.h"
using namespace std;
static trieTree  * keyWords = NULL;
static bool KeyChar[256] = {0};
#define ADD_KW(k) keyWords->insertNCase((unsigned char*)(k),(void*)(unsigned long)1);
void initKeyWords()
{
    keyWords = new trieTree;
    memset(KeyChar,0,sizeof(KeyChar));
    KeyChar['(']=true;
    KeyChar['.']=true;
    KeyChar['`']=true;
    KeyChar['\'']=true;
    KeyChar['"']=true;
    KeyChar['+']=true;
    KeyChar['-']=true;
    KeyChar['*']=true;
    KeyChar['/']=true;
    KeyChar['(']=true;
    KeyChar[')']=true;
    KeyChar['{']=true;
    KeyChar['=']=true;
    KeyChar['>']=true;
    KeyChar['<']=true;
    KeyChar[';']=true;

    for(int i=0;i<256;i++)
    {
        char tmp[2]={0};
        if(KeyChar[i])
        {
            tmp[0]=i;
            ADD_KW(tmp);
        }
    }
    ADD_KW("PRIMARY")
    ADD_KW("UNIQUE");
    ADD_KW("KEY");
    ADD_KW("AS");
    ADD_KW("FOREIGN");
    ADD_KW("USING");
    ADD_KW("SELECT");
    ADD_KW("INSERT");
    ADD_KW("DELETE");
    ADD_KW("UPDATE");
    ADD_KW("CREATE");
    ADD_KW("ALTER");
    ADD_KW("FROM");
    ADD_KW("DROP");
    ADD_KW("ADD");
    ADD_KW("TABLE");
    ADD_KW("DATABASE");
    ADD_KW("OR");
    ADD_KW("NOT");
    ADD_KW("JOIN");
    ADD_KW("ORDER");
    ADD_KW("DESC");
    ADD_KW("BY");
    ADD_KW("ASC");
    ADD_KW("BETWEEN");
    ADD_KW("UNION");
    ADD_KW("NULL");
    ADD_KW("EXCEPT");
    ADD_KW("MAX");
    ADD_KW("MIN");
    ADD_KW("AVG");
    ADD_KW("SUM");
    ADD_KW("COUNT");
    ADD_KW("DISTINCT");
    ADD_KW("GROUP BY");
    ADD_KW("HAVING");
    ADD_KW("RESTRICT");
    ADD_KW("CASCADE");
    ADD_KW("TO");
}
void destroyKeyWords()
{
    if(keyWords)
    {
        delete keyWords;
        keyWords = NULL;
    }
}
bool isKeyWord(const char * str,uint32_t size)
{
    if(keyWords->findNCase((const unsigned char*)str,size)!=NULL)
        return true;
    else
        return false;
}
bool isKeyChar(uint8_t c)
{
    return KeyChar[c];
}
const char * endOfWord(const char * str)
{
    while (!isSpaceOrComment(str) && *str != '\0'&&!isKeyChar(*str))
        str++;
    return str;
}


