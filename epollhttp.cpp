#include"wrap.h"
int main(int argc,char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    if(argc!=2)
    {
        std::cerr<<"./demo rootpath"<<std::endl;
        return 0;
    }
    chdir(argv[1]);     //chdir出错判断
    HttpServer hserver;
    hserver.epollstart();
    return 0;
}