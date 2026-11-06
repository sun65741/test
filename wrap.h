#include<iostream>
#include<error.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<string.h>
#include<unistd.h>
#include<ctype.h>
#include<string>
#include<signal.h>
#include <sys/wait.h>
#include <sys/select.h>
#include<sys/epoll.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<vector>
#include<algorithm>
#include<strings.h>
#include<dirent.h>
#include <sys/sendfile.h>
#include<unordered_map>
#include<memory>
#include<set>
#include<netinet/tcp.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <sys/eventfd.h>
#include<thread>
int Socket(int domain, int type, int protocol);
int Bind(int sockfd, const struct sockaddr *addr,socklen_t addrlen);
int Listen(int sockfd, int backlog);
int Accept4(int sockfd, struct sockaddr *addr,socklen_t *addrlen,int flags);    //-1错误 -2缓冲区空 
int Epoll_create(int size);
int Epoll_ctl(int epfd, int op, int fd,struct epoll_event *event);
int Epoll_wait(int epfd, struct epoll_event *events,int maxevents, int timeout);
const char* get_file_type(const char* file);
class HttpServer;
class Worker;
class Blockqueue{       //自定义数据
public:
    void push(int fd)
    {
        std::lock_guard<std::mutex> lock(mtx);
        fd_queue.push(fd);
        return;
    }
    bool pop(int& fd)
    {
        std::lock_guard<std::mutex> lock(mtx);
        if(fd_queue.empty())
        {
            return false;
        }
        fd = fd_queue.front();
        fd_queue.pop();
        return true;
    }
private:
    std::queue<int> fd_queue;
    std::mutex mtx;
};
class Conn{
private:
    int fd=-1;              //cfd连接文件描述符
    Worker* srv = nullptr;   //  反向指针
    char rbuf[8192];        //读缓冲
    int rlen=0;             //读的字符长度
    int spos=0;             //消费的字符起始
    enum class Stat{REQ_LINE,HEAD,DONE}phase;     //解析状态 REQ_LINE 等解析请求行  HEAD 行已被解析，解析头部  DONE 头部读完
    char method[16];    
    char path[256];
    char protocol[16];
    //--------------------------------
    std::string wbuf;       //待发送头部
    size_t wpos=0;          //wbuf发送的偏移
    int file_fd=-1;         //要发送文件的fd，-1为没有
    off_t file_off=0;       //已发送文件的偏移
    off_t file_size=0;      //发送文件的大小

    int try_sendall();       //尝试发送所有储存在Conn里的信息并记录下来偏移 -1错误 -2缓冲区满等待 0发送完全
    int analyze_rbuf(char *buf,int buflen);             //-1请求行过长错误  0代表rbuf没满一行返回   1代表成功解析                 
    int pack_header(const char* f,int no,const char *desp,const char *type,struct stat& sbuf);        //打包要发送的协议头并且发送
    void build_body(const char *file);               //打包文件的属性状态
    void build_dir(const char* dir,std::string& html);         //打包文件的所需东西
    void do_read();                         //read到http的请求并处理解析
    void http_request(const char *file);    //处理http请求文件的具体操作逻辑

public:
    time_t last_active;
    explicit Conn(int c,Worker* s):fd(c),srv(s)
    {
        rlen = 0;
        spos = 0;
        phase = Stat::REQ_LINE;
        memset(method, 0, sizeof(method));
        memset(path, 0, sizeof(path));
        memset(protocol, 0, sizeof(protocol));
        last_active = time(nullptr);
        wpos = 0;
        file_fd = -1;
        file_off = 0;
        file_size = 0;
        last_active=time(nullptr);
    }
    ~Conn() { closeallfd(); }
    void handle_event(uint32_t events);      //处理自己缓冲区的一个函数
    int fdg() const {return fd;}
    void closeallfd();                      //关闭自己的所有连接
    void reset_conn();
    void re_time();
};     
class HttpServer{
private:
    int lfd;        //监听文件描述符
    std::vector<std::shared_ptr<Worker>> workers;
    int efd;        //Epoll create红黑树的根
    std::unordered_map<int,std::shared_ptr<Conn>> clients;
    void listencreat();         //端口我这次固定9876不改了
    int epolladd();            //lfd监听到后返回cfd

public:
    std::set<std::pair<time_t, int>> timer_set;
    HttpServer()
    {
        lfd=-1;
        efd=-1;
    }
    void epollstart();          //epoll树创建和循环wait
    void disconn(Conn* c);      //解除连接
    void update_timer(int fd,time_t old_time,time_t new_time);
};
class Worker{
public:
    Worker(int f):workid(f)
    {
        efd=Epoll_create(1);    //创建属于这个线程的epoll树

        wakeup_fd=eventfd(0,EFD_NONBLOCK);    //唤醒这个线程的fd
        if(wakeup_fd==-1)
            return;
        epoll_event tmp;
        tmp.events=EPOLLIN|EPOLLET; tmp.data.fd=wakeup_fd;
        Epoll_ctl(efd,EPOLL_CTL_ADD,wakeup_fd,&tmp);
    }
    void run();
    void disconn(Conn* c);
    void addConn(int cfd);      //往任务队列中添加任务并且唤醒线程
    int setepoll(int fd,uint32_t events);       //设置epoll事件
    void update_timer(int fd,time_t old_time,time_t new_time);
private:
    int workid;
    int efd;
    int wakeup_fd;
    Blockqueue fd_queue;
    std::unordered_map<int,std::shared_ptr<Conn>> clients;
    std::set<std::pair<time_t, int>> timer_set;
    int Workerepolladd(int cfd);            //在queue中拿去并且挂到自己的树上
};