#include <stdio.h>
#include <stdlib.h>
#include <string.h>
void hex2str(const char* hex)
{
	int size =1+strlen(hex)/2;
	char * str = (char*)malloc(size);
	for(int i=0;i<size-1;i++)
	{
	    str[i]=0;
		if(hex[i*2]>='A'&&hex[i*2]<='F')
		    str[i] = hex[i*2]-'A'+10;
		else if(hex[i*2]>='a'&&hex[i*2]<='f')
          str[i] = hex[i*2]-'a'+10;
      else if(hex[i*2]>='0'&&hex[i*2]<='9')
          str[i] = hex[i*2]-'0';
      else
          printf("bad format\n");
		str[i]<<=4;
        if(hex[1+i*2]>='A'&&hex[1+i*2]<='F')
            str[i] |= hex[1+i*2]-'A'+10;
        else if(hex[1+i*2]>='a'&&hex[1+i*2]<='f')
          str[i] |= hex[1+i*2]-'a'+10;
        else if(hex[1+i*2]>='0'&&hex[1+i*2]<='9')
          str[i] |= hex[1+i*2]-'0';
        else
          printf("bad format\n");
	}
	str[size-1]='\0';
	printf("%s\n",str);
	free(str);
}
int main(int argc,char*argv[])
{
    hex2str(argv[1]);
}
