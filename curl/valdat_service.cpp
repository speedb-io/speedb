#include <machine/endian.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <list>
#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <evhttp.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/bufferevent.h>

using namespace std;

using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;



class ValdatReplication {
 public:
  ValdatReplication(int id) : init_(true), done_(false), progress_key_(0), id_(id) {
    write_thread_ = std::thread(&ValdatReplication::writeFunc, this);
  }
  ~ValdatReplication() {
    done_.store(true);
    write_thread_.join();    
  }
private:  
  void writeFunc() {
    fillup();
    
    while (!done_.load()) {
      sleep(5);  
      writeCP();
    }

    return;
  }  
  void fillup() {
    uint64_t file_size = 0;
    while (file_size < 2048) {
      int rc;
      FILE *fp;
      char buffer[512];
      memset(buffer, 0 ,512);
      sprintf(buffer, "%llu", file_size);

      fp = fopen("valdat_src","a");
      rc = fwrite(buffer,512,1,fp);
      fclose(fp);
      file_size++;
    }
    init_ = false;

  }
  void writeCP() {
    /*uint64_t file_size = 0;
    while (file_size < 2048) {
      int rc;
      FILE *fp;
      char buffer[512];
      memset(buffer, 0 ,512);
      sprintf(buffer, "%llu", file_size);

      fp = fopen("valdat_src","a");
      rc = fwrite(buffer,512,1,fp);
      fclose(fp);
      file_size++;
    }*/
    progress_key_++;

  }  

 public:
  bool init_ = true;
  uint64_t progress_key_;
  std::atomic<bool> done_;
  int id_;
  std::mutex mutex_;
  std::thread write_thread_;
};

std::list<ValdatReplication*> valdate_rep_list_;
std::atomic<int> demo_done_ = 0;
std::mutex demo_mutex_;
std::condition_variable demo_cv_;
std::atomic<int> repId_ = 0;


void createRep() {
  int id = repId_.fetch_add(1);
  ValdatReplication* valdate_rep = new ValdatReplication(id);
  valdate_rep_list_.push_front(valdate_rep);
}

void deleteRep() {
  ValdatReplication* valdate_rep = valdate_rep_list_.back();
  valdate_rep_list_.pop_back();

  delete valdate_rep;
  if (valdate_rep_list_.size() == 0)
    demo_done_.store(1);

}

void clear() {
  demo_done_.store(1);
}
void http_request_done(struct evhttp_request *req, void *arg){
    char buf[1024];
    int s = evbuffer_remove(req->input_buffer, &buf, sizeof(buf) - 1);
    buf[s] = '\0';
    printf("%s", buf);
    // terminate event_base_dispatch()
    if (strcmp(req->uri, "/createRep") == 0) {
      createRep();
    }
    if (strcmp(req->uri, "/clear") == 0) {
      clear();
      event_base_loopbreak((struct event_base *)arg);

    }    
    if (strcmp(req->uri, "/deleteRep") == 0) {
      deleteRep();
      event_base_loopbreak((struct event_base *)arg);
    }    
  }






static void
HttpCreateRep(struct evhttp_request *req, void *arg) {
  createRep();
}

int main(int argc, char** argv) {
  /*if (argc != 2) {
    cerr << "Usage: " << argv[0] << " db_dir\n";
    return -1;
  }*/
  //createRep();
  //deleteRep();
  struct event_base *base;
  struct evhttp_connection *conn;
  struct evhttp_request *req;

  base = event_base_new();
  conn = evhttp_connection_base_new(base, NULL, "127.0.0.1", 9900);
  req = evhttp_request_new(http_request_done, base);

  evhttp_add_header(req->output_headers, "Host", "localhost");
  //evhttp_add_header(req->output_headers, "Connection", "close");
	//evhttp_set_cb(http, "/createRep", HttpCreateRep, NULL);

  evhttp_make_request(conn, req, EVHTTP_REQ_GET, "/createRep");
  evhttp_connection_set_timeout(req->evcon, 600);
  event_base_dispatch(base);
  req = evhttp_request_new(http_request_done, base);

  evhttp_add_header(req->output_headers, "Host", "localhost");
  //evhttp_add_header(req->output_headers, "Connection", "close");
	//evhttp_set_cb(http, "/createRep", HttpCreateRep, NULL);
  sleep(20); // 
  evhttp_make_request(conn, req, EVHTTP_REQ_GET, "/deleteRep");
  evhttp_connection_set_timeout(req->evcon, 600);
  event_base_dispatch(base);
  //evhttp_make_request(conn, req, EVHTTP_REQ_GET, "/clear");

  std::unique_lock<std::mutex> demo_wait_lck(demo_mutex_);
  while (!demo_done_.load()) {
    demo_cv_.wait(demo_wait_lck);
  }
  return 0;
}
