#include"wrap.h"
void sys_err(const char * s)
{
    perror(s);
    exit(1);
}
int Socket(int domain, int type, int protocol) 
{
    int fd = ::socket(domain, type, protocol);
    if (fd < 0) 
    {
        perror("socket");
    }
    return fd;
}
int Bind(int fd, const struct sockaddr* addr, socklen_t len) 
{
    int ret;
    if ((ret=::bind(fd, addr, len)) < 0)
    {
        perror("bind");
    }
    return ret;
}
int Listen(int fd, int backlog) 
{
    int ret;
    if ((ret=::listen(fd, backlog)) < 0) 
    {
        perror("listen");
    }
    return ret;
}
int Accept4(int sockfd, struct sockaddr *addr,socklen_t *addrlen,int flags)
{
    for(;;)
    {
        int cfd=::accept4(sockfd,addr,addrlen,flags);
        if(cfd>0)return cfd;
        else
        {
            if(errno==EINTR)continue;
            if(errno==EAGAIN||errno==EWOULDBLOCK)return -2;
            perror("Accept4 error");
            return -1;
        }
    }
}
int Epoll_create(int size)      
{
    for(;;)
    {
        int ret=epoll_create(size);
        if(ret<0)
        {
            if(errno==EINTR)continue;
            perror("Epoll_creat error");
        }
        return ret;
    }
}
int Epoll_ctl(int epfd, int op, int fd,struct epoll_event *event)
{
    for(;;)
    { 
        int ret=epoll_ctl(epfd,op,fd,event);
        if(ret<0)
        {
            if(errno==EINTR)continue;
            perror("Epoll_ctl error");
        }
        return ret;
    }
}
int Epoll_wait(int epfd, struct epoll_event *events,int maxevents, int timeout)
{
    for(;;)
    {
        int ret=epoll_wait(epfd,events,maxevents,timeout);
        if(ret<0)
        {
            if(errno==EINTR)continue;
            perror("Epoll_wait error");
        }
        return ret;
    }
}
const char* get_file_type(const char* file)
{
    std::string fil(file);
    
    // 1. 找最后一个 '.' 的位置
    size_t n = fil.rfind('.');

    if (n == std::string::npos || n == fil.size() - 1) {
        return "text/plain; charset=utf-8"; // 默认二进制流（浏览器会当附件下载）
    }
    
    std::string ext = fil.substr(n + 1);
    
    for (char& c : ext) c = tolower(c);
    
    // 文本类（带上 charset 防中文乱码）
    if (ext == "html" || ext == "htm") return "text/html; charset=utf-8";
    if (ext == "css") return "text/css; charset=utf-8";
    if (ext == "js")  return "application/javascript; charset=utf-8";
    if (ext == "txt") return "text/plain; charset=utf-8";
    
    // 图片类
    if (ext == "png") return "image/png";
    if (ext == "jpg" || ext == "jpeg") return "image/jpeg";
    if (ext == "gif") return "image/gif";
    if (ext == "ico") return "image/x-icon";
    if (ext == "svg") return "image/svg+xml";

    // 视频类（比如你需要的 MP4）
    if (ext == "mp4") return "video/mp4";
    if (ext == "webm") return "video/webm";
    if (ext == "ogg") return "video/ogg";

    // 未知类型，给个默认值
    return "text/plain; charset=utf-8";
}
//Conn-------------------------------
void Conn::handle_event(uint32_t events)           //处理自己缓冲区的一个函数  
{
    int ret=0;
    if (events & (EPOLLERR | EPOLLHUP)) 
    {
        srv->disconn(this);
        return;
    }
    if(events&EPOLLIN)
    {
        re_time();
        this->do_read();
        return;
    }
    if(events&EPOLLOUT)
    {
        re_time();
        ret=this->try_sendall();
        if (ret==-1) 
        {                //-1error disconn
            srv->disconn(this);
            return;
        }
        else if(ret==0)
        {
            reset_conn();
        }
    }
}
int Conn::analyze_rbuf(char *buf,int buflen)        //-1请求行过长错误  0代表rbuf没满一行返回   1代表成功解析
{
    char *n=(char *)memchr(rbuf,'\n',rlen);
    if(!n)
    {
        if(rlen>=sizeof(rbuf))
            return -1;  //未找到\n并且rbuf里面也已经装满，请求行过长（异常）返回-1
        return 0;   //未找到\n 代表缓冲区没满一行返回0
    }

    size_t linelen=n-rbuf;      //不包含/n的行长度到/r
    size_t copy=linelen;
    if((int)linelen>0&&rbuf[linelen-1]=='\r')--copy;
    if(copy>=buflen)return-1;       //buflen不够返回-1
    memcpy(buf,rbuf,copy);
    buf[copy]='\0';

    spos=linelen+1;  //连同\n一起消费掉

    if(spos>0&&spos<=rlen)
    {
        memmove(rbuf,rbuf+spos,rlen-spos);
        rlen-=spos;
        spos=0;
    }
    return 1;
}
void Conn::do_read()      //read到http的请求并处理解析
{
    //读取缓冲区发来的所有东西
    for(;;)
    {
        int n=read(this->fd,this->rbuf+this->rlen,sizeof(this->rbuf)-rlen);
        if(n>0)
        {
            rlen+=n;
            if(rlen==sizeof(rbuf))      //rbuf满了还没有读完
            {
                std::cerr<<"request too bigggg!!!"<<std::endl;
                srv->disconn(this);
                return;
            }
        }
        else if(n==0)       //对端正常关闭
        {
            srv->disconn(this);
            return;
        }
        else
        {
            if(errno==EINTR)continue;
            if(errno==EAGAIN||errno==EWOULDBLOCK)break;
            perror("read error");
            srv->disconn(this);
            return;
        }
    }

    //解析rbuf里面的数据
    for(;;)
    {
        char line[1024];
        if(phase==Stat::REQ_LINE)
        {
            int n=analyze_rbuf(line,1024);
            if(n==0)return;
            if(n==-1)
            {
                srv->disconn(this);
                return;
            }
            sscanf(line,"%15s %255s %15s",method,path,protocol);
            //std::cerr<<"!!!  method:"<<method<<" path:"<<path<<" protocol:"<<protocol<<std::endl;
            phase=Stat::HEAD;
        }
        else if(phase==Stat::HEAD)
        {
            int n=analyze_rbuf(line,1024);
            if(n==0)return;
            if(n==-1)
            {
                srv->disconn(this);
                return;
            }
            if(line[0]=='\0')
            {
                phase=Stat::DONE;
                break;
            }
            //std::cerr<<line<<std::endl;
        }
        else
        {
            break;
        }
    }

    //处理解析过后的数据
    if(strncasecmp(method,"GET",3)==0)
    {
        const char* file=path+1;
        if(strcmp(path,"/")==0)file="./";
        http_request(file);
    }
}
void Conn::http_request(const char *file)        //处理http请求文件的具体操作逻辑
{
    struct stat sbuf;
    int ret=stat(file,&sbuf);
    if(ret!=0)
    {
        stat("404.html",&sbuf);
        const char *type=get_file_type("404.html");
        pack_header("404.html",404,"not found ",type,sbuf);
        return;
    }
    const char *type=get_file_type(file);
    ret=pack_header(file,200,"ok ",type,sbuf);
    if(ret==0)reset_conn();
    if(ret==-1)srv->disconn(this);
    return;
}
int Conn::try_sendall()       //尝试发送所有储存在Conn里的信息并记录下来偏移 -1错误 -2缓冲区满等待 0发送完全
{
    while(wpos<wbuf.size())     //send wbuf
    {
        ssize_t n=send(fd,wbuf.c_str()+wpos,wbuf.size()-wpos,MSG_DONTWAIT);
        if(n<=0)
        {
            if(errno==EINTR)continue;
            if(errno==EAGAIN||errno==EWOULDBLOCK)
            {
                if(srv->setepoll(fd,EPOLLOUT|EPOLLET)!=0)return -1;
                return -2;
            }
            return -1;
        }
        wpos+=n;
    }
    while(file_fd>=0&&file_off<file_size)   //send file
    {
        ssize_t n=sendfile(fd,file_fd,&file_off,file_size-file_off);
        if(n<=0)
        {
            if(errno==EINTR)continue;
            if(errno==EAGAIN||errno==EWOULDBLOCK)
            {
                if(srv->setepoll(fd,EPOLLOUT|EPOLLET)!=0)return -1;
                return -2;
            }
            return -1;
        }
    }
    return 0;
}
int Conn::pack_header(const char* f,int no,const char *desp,const char *type,struct stat& sbuf)       //打包要发送的协议头并且发送 -1error -2buf满 0正常发完
{
    off_t sbuflen=sbuf.st_size;
    std::string html;
    if(S_ISDIR(sbuf.st_mode))
    {
        type="text/html; charset=utf-8";
        build_dir(f,html);
        sbuflen=html.size();
    }
    std::string strhead;
    strhead="HTTP/1.1 "+std::to_string(no)+" "+desp+"\r\n";
    strhead+="Content-Type: "+std::string(type)+"\r\n";
    strhead+="Content-Length: "+std::to_string((long long)sbuflen)+"\r\n";
    strhead += "Connection: keep-alive\r\n";
    strhead+="\r\n";
    wbuf=strhead+html;

    if(S_ISREG(sbuf.st_mode))
    {
        build_body(f);
    }
    return try_sendall();
}
void Conn::build_body(const char *file)          //打包文件的属性状态
{
    file_fd=open(file,O_RDONLY);
    struct stat filebuf;
    stat(file,&filebuf);
    file_size=filebuf.st_size;
    return;
}
void Conn::build_dir(const char* dir,std::string& html)            //打包文件的所需东西
{
    int i,ret;
    std::string d(dir);
    if (d.empty() || d.back() != '/') d += '/';
    html+="<!doctype html><html><head><meta charset=\"utf-8\"><title>"+d+"</title></head>";
    html+="<body><h1>当前目录:"+d+"</h1><table>";
    struct dirent **ptr;
    int num=scandir(dir,&ptr,NULL,alphasort);
    if (num < 0) 
    {
        html += "<tr><td>无法读取目录</td></tr>";
    } 
    else
    {
        for(i=0;i<num;i++)
        {
            std::string name=ptr[i]->d_name;
            std::string full=d+name;
            if (name == "." || name == ".." || name[0] == '.') 
            {
                free(ptr[i]);
                continue;
            }
            struct stat st;
            stat(full.c_str(),&st);
            if(S_ISREG(st.st_mode))
            {
                html+="<tr><td><a href=\""+name+"\">"+full+"</a></td><td>"+std::to_string(st.st_size)+"</td></tr>";
            }
            else if(S_ISDIR(st.st_mode))
            {
                html+="<tr><td><a href=\""+name+"/\">"+full+"/</a></td><td>"+std::to_string(st.st_size)+"</td></tr>";
            }
            free(ptr[i]);
        }
        free(ptr);
    }
    html += "</table></body></html>";
    return;
}
void Conn::closeallfd()                 //关闭自己的所有连接
{
    if(this->fd>=0)
    {
        ::close(this->fd);
        this->fd=-1;
    }
    if(this->file_fd>=0)
    {
        ::close(this->file_fd);
        this->file_fd=-1;
    }
}
void Conn::reset_conn()
{
    wpos=0;
    file_off=0;
    rlen=0;
    if(file_fd>=0)close(file_fd);
    file_fd=-1;
    phase=Stat::REQ_LINE;
    last_active=time(nullptr);
    srv->setepoll(fd,EPOLLIN|EPOLLET);
}
void Conn::re_time()
{
    time_t now=time(nullptr);
    srv->update_timer(fd,last_active,now);
    last_active=now;
}
//HttpServer------------------------
void HttpServer::listencreat()          //端口我这次固定9876不改了
{
    int ret=0;
    this->lfd=Socket(AF_INET,SOCK_STREAM,0);
    if(this->lfd<0)sys_err("this->lfd creat error");

    //设置端口复用
    int op=1;
    setsockopt(this->lfd,SOL_SOCKET,SO_REUSEPORT,&op,sizeof(op));

    //Bind绑定地址
    sockaddr_in serv_addr;
    serv_addr.sin_family=AF_INET;
    serv_addr.sin_port=htons(9876);
    serv_addr.sin_addr.s_addr=htonl(INADDR_ANY);
    ret=Bind(this->lfd,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    if(ret<0)sys_err("Bind error");

    Listen(this->lfd,128);
    
    int flags=fcntl(this->lfd,F_GETFL,0);
    fcntl(this->lfd,F_SETFL,flags|O_NONBLOCK);
    return;
}
void HttpServer::epollstart()           //epoll树创建和循环wait
{
    int ret;
    listencreat();
    this->efd=Epoll_create(1);
    if(this->efd<0)sys_err("Epoll_create error");
    epoll_event tmp;
    tmp.events=EPOLLIN|EPOLLET;     tmp.data.fd=lfd;
    ret=Epoll_ctl(this->efd,EPOLL_CTL_ADD,this->lfd,&tmp);

    epoll_event events[1024];       //max epoll 1024
    while(1)
    {
        int n=Epoll_wait(this->efd,events,1024,2000);
        auto it=timer_set.begin();
        time_t now=time(nullptr);
        while(it!=timer_set.end())
        {
            if((now-it->first)>=30)
            {
                /*std::cerr << "timeout fd=" << it->second
                  << " last=" << it->first
                  << " now=" << now
                  << " diff=" << (now-it->first) << std::endl;*/
                int fd = it->second;
                auto client_it = clients.find(fd);
                if (client_it != clients.end()) 
                {
                    disconn(client_it->second.get());
                    it = timer_set.begin(); 
                    continue;
                }
                else
                {
                    // 异常情况：clients 里没这个 fd，但 timer_set 里有（脏数据）
                    it = timer_set.erase(it);
                }
            }
            else
            {
                break;      //set 是有序的
            }
        }
        if(n>0)
        {
            for(int i=0;i<n;i++)
            {
                if(events[i].data.fd==this->lfd)
                {
                    this->epolladd();
                }
                if(events[i].data.fd!=this->lfd)
                {
                    auto it=clients.find(events[i].data.fd);
                    if(it==clients.end())continue;
                    auto c=it->second;
                    c->handle_event(events[i].events);
                }
            }
        }
    }
}
void HttpServer::epolladd()             //this->lfd监听到后建立连接
{
    sockaddr_in cli_addr;
    socklen_t cli_len;
    for(;;)
    {
        cli_len=sizeof(cli_addr);
        int cfd=Accept4(this->lfd,(sockaddr*)&cli_addr,&cli_len,SOCK_NONBLOCK);
        if(cfd==-2)break;       //-2代表缓冲区空 break退出循环
        if(cfd==-1)return;      //处理Accept4的错误，省略不写了 

        int opt = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        auto c=std::make_shared<Conn>(cfd,this);
        this->clients[cfd]=c;
        timer_set.insert(std::make_pair(c->last_active,cfd));

        //将新的cfd挂上数
        epoll_event tmp;
        tmp.events=EPOLLIN|EPOLLET;     tmp.data.fd=cfd;   //113 21:55//给的是裸指针 shared智能管理
        int ret=Epoll_ctl(this->efd,EPOLL_CTL_ADD,cfd,&tmp);
        if(ret<0)this->disconn(c.get());
    }
}
int HttpServer::setepoll(int fd,uint32_t events)          //设置epoll事件   -1代表错误  0代表成功设置
{
    epoll_event ev;
    ev.events   = events;
    ev.data.fd=fd;
    return Epoll_ctl(efd, EPOLL_CTL_MOD, fd, &ev);
}
void HttpServer::update_timer(int fd,time_t old_time,time_t new_time)
{
    auto it=timer_set.find({old_time,fd});
    if(it!=timer_set.end())
    {
        timer_set.erase(it);
    }
    timer_set.insert({new_time, fd});
}
void HttpServer::disconn(Conn* c)       //解除连接 
{
    int fd=c->fdg();
    timer_set.erase({c->last_active,fd});
    Epoll_ctl(efd, EPOLL_CTL_DEL, fd, nullptr);
    c->closeallfd();
    clients.erase(fd);
}