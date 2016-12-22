#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>
#include <map>
#include <utility>


#define CA_FILE "./cert/ca.pem"
#define USERFILE "./etc/passwd"
#define GROUPFILE "./etc/group"


class yfs_client {
  extent_client *ec;
  lock_client *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST,
  			NOPEM, ERRPEM, EINVA, ECTIM, ENUSE };

  typedef int status;


  struct filestat {
    unsigned long long size;
    unsigned long mode;
    unsigned short uid;
    unsigned short gid;
  };

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
    unsigned long mode;
    unsigned short uid;
    unsigned short gid;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
    unsigned long mode;
    unsigned short uid;
    unsigned short gid;
  };

  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

  class DirTable {
   private:
     std::map<std::string, inum> table;
   public:
     DirTable(std::string);
     std::string dump();
     bool lookup(std::string, inum&);
     void insert(std::string, inum);
     void list(std::list<dirent>&);
     void erase(std::string);
  };

 public:
  yfs_client();
  yfs_client(std::string, std::string, const char*);
  yfs_client(std::string, std::string);
  yfs_client(std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool issymlink(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, filestat, unsigned long);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  int verify(const char* cert_file, unsigned short*);
  
  /** you may need to add symbolic link related methods here.*/
  int readlink(inum, std::string &);
  int symlink(inum, const char *, const char *, inum &);

  void commit();
  void undo();
  void redo();
};

#endif 
