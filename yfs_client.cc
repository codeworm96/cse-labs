// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/x509.h>
#include <openssl/pem.h>

#define MAX_NAME_LEN 30

yfs_client::yfs_client()
{
  ec = NULL;
  lc = NULL;
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst, const char* cert_file)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
}

int
yfs_client::verify(const char* name, unsigned short *uid)
{
    int ret = ERRPEM;
    X509_STORE *ca_ctx = NULL;
    X509 * cert = NULL;
    BIO * c = NULL;
    X509_STORE_CTX *csc = NULL;

    OpenSSL_add_all_algorithms();

    ca_ctx = X509_STORE_new();
    if (ca_ctx == NULL) {
        EVP_cleanup();
        return ERRPEM;
    }

    if(!X509_STORE_load_locations(ca_ctx, CA_FILE, NULL)) {
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }

    c = BIO_new(BIO_s_file());
    if (c == NULL) {
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }
    if (BIO_read_filename(c, name) <= 0) {
        BIO_free(c);
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }

    cert = PEM_read_bio_X509_AUX(c, NULL, NULL, NULL);
    if (cert == NULL) {
        BIO_free(c);
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }

    csc = X509_STORE_CTX_new();
    if (csc == NULL) {
        X509_free(cert);
        BIO_free(c);
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }

    X509_STORE_set_flags(ca_ctx, 0);
    if (!X509_STORE_CTX_init(csc, ca_ctx, cert, NULL)) {
        X509_STORE_CTX_free(csc);
        X509_free(cert);
        BIO_free(c);
        X509_STORE_free(ca_ctx);
        EVP_cleanup();
        return ERRPEM;
    }

    if (X509_verify_cert(csc)) {
        //ok
        char buf[MAX_NAME_LEN];
        X509_NAME *name = X509_get_subject_name(cert);
        X509_NAME_get_text_by_NID(name, OBJ_txt2nid("CN"), buf, MAX_NAME_LEN); 
 
        std::string cn(buf);
        std::ifstream pw(USERFILE);
        std::string line;
        ret = ENUSE;
        while(std::getline(pw, line)) {
            size_t pos = line.find_first_of(":");
            if (cn == line.substr(0, pos)) {
                pos += 3;
		size_t next = line.find_first_of(":", pos);
                *uid = std::atoi(line.substr(pos, next - pos).c_str());
                ret = OK;
                break;
            }
	}
    } else {
        int err = X509_STORE_CTX_get_error(csc);
        if (err == X509_V_ERR_CERT_HAS_EXPIRED) {
            ret = ECTIM;
        } else if (err = X509_V_ERR_CERT_UNTRUSTED) {
            ret = EINVA;
        } else {
            ret = ERRPEM;
        }
    }

    X509_STORE_CTX_free(csc);
    X509_free(cert);
    BIO_free(c);
    X509_STORE_free(ca_ctx);
    EVP_cleanup();
    return ret;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

yfs_client::DirTable::DirTable(std::string s)
{
  size_t cur = 0;
  size_t next;
  while (cur < s.size()) {
    next = s.find('/', cur);
    std::string name = s.substr(cur, next - cur);
    cur = next + 1;

    next = s.find('/', cur);
    inum id = n2i(s.substr(cur, next - cur));
    cur = next + 1;

    table.insert(std::pair<std::string, inum>(name, id));
  }
}

std::string
yfs_client::DirTable::dump()
{
  std::string res = "";
  for (std::map<std::string, inum>::iterator it = table.begin(); it != table.end(); ++it) {
    res = res + it->first + "/" + filename(it->second) + "/";
  }
  return res;
}

bool
yfs_client::DirTable::lookup(std::string name, inum& res)
{
  std::map<std::string, inum>::iterator it = table.find(name);
  if (it != table.end()) {
    res = it->second;
    return true;
  } else {
    return false;
  }
}

void
yfs_client::DirTable::insert(std::string name, inum id)
{
  table.insert(std::pair<std::string, inum>(name, id));
}

void
yfs_client::DirTable::list(std::list<dirent> & l)
{
  for (std::map<std::string, inum>::iterator it = table.begin(); it != table.end(); ++it) {
    struct dirent entry;
    entry.name = it->first;
    entry.inum = it->second;
    l.push_back(entry);
  }
}

void
yfs_client::DirTable::erase(std::string name)
{
    table.erase(name);
}

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a directory\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symbolic link\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symbolic link\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, filestat st, unsigned long toset)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    lc->acquire(ino);
    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
      printf("setattr: file not exist\n");
      lc->release(ino);
      return r;
    }

    buf.resize(st.size);

    r = ec->put(ino, buf);
    if (r != OK) {
      printf("setattr: update failed\n");
      lc->release(ino);
      return r;
    }

    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    lc->acquire(parent);
    std::string buf;
    r = ec->get(parent, buf);
    if (r != OK) {
      printf("create: parent directory not exist\n");
      lc->release(parent);
      return r;
    }

    inum id;
    DirTable table(buf);
    if (table.lookup(std::string(name), id)) {
      printf("create: already exist\n");
      lc->release(parent);
      return EXIST;
    }

    r = ec->create(extent_protocol::T_FILE, ino_out);
    if (r != OK) {
      printf("create: creation failure\n");
      lc->release(parent);
      return r;
    }
    
    table.insert(std::string(name), ino_out);
    r = ec->put(parent, table.dump());
    lc->release(parent);
    if (r != OK) {
      printf("create: parent directory update failed\n");
      return r;
    }

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    lc->acquire(parent);
    std::string buf;
    r = ec->get(parent, buf);
    if (r != OK) {
      printf("mkdir: parent directory not exist\n");
      lc->release(parent);
      return r;
    }

    inum id;
    DirTable table(buf);
    if (table.lookup(std::string(name), id)) {
      printf("mkdir: already exist\n");
      lc->release(parent);
      return EXIST;
    }

    r = ec->create(extent_protocol::T_DIR, ino_out);
    if (r != OK) {
      printf("mkdir: creation failure\n");
      lc->release(parent);
      return r;
    }
    
    table.insert(std::string(name), ino_out);
    r = ec->put(parent, table.dump());
    lc->release(parent);
    if (r != OK) {
      printf("mkdir: parent directory update failed\n");
      return r;
    }

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    lc->acquire(parent);
    std::string buf;
    r = ec->get(parent, buf);
    lc->release(parent);
    if (r != OK) {
      printf("lookup: parent directory not exist\n");
      return r;
    }

    DirTable table(buf);
    found = table.lookup(std::string(name), ino_out);
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    lc->acquire(dir);
    std::string buf;
    r = ec->get(dir, buf);
    lc->release(dir);
    if (r != OK) {
      printf("readdir: directory not exist\n");
      return r;
    }

    DirTable table(buf);
    table.list(list);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    lc->acquire(ino);
    std::string buf;
    r = ec->get(ino, buf);
    lc->release(ino);
    if (r != OK) {
      printf("read: file not exist\n");
      return r;
    }

    if (off < (int)buf.size()) {
      data = buf.substr(off, size);
    } else {
      data = "";
    }

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    lc->acquire(ino);
    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
      printf("write: file not exist\n");
      lc->release(ino);
      return r;
    }

    if (buf.size() < off + size) {
      buf.resize(off + size);
    }

    for (unsigned int i = 0; i < size; ++i) {
      buf[off + i] = data[i];
    }

    r = ec->put(ino, buf);
    lc->release(ino);
    if (r != OK) {
      printf("write: failed\n");
      return r;
    }
    bytes_written = size;

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    lc->acquire(parent);
    std::string buf;
    r = ec->get(parent, buf);
    if (r != OK) {
      printf("unlink: parent directory not exist\n");
      lc->release(parent);
      return r;
    }

    inum id;
    DirTable table(buf);
    if (!table.lookup(std::string(name), id)) {
      printf("unlink: file not found\n");
      lc->release(parent);
      return NOENT;
    }

    table.erase(std::string(name));
    r = ec->put(parent, table.dump());
    lc->release(parent);
    if (r != OK) {
      printf("unlink: parent directory update failed\n");
      return r;
    }

    lc->acquire(id);
    r = ec->remove(id);
    lc->release(id);
    if (r != OK) {
      printf("unlink: remove failed\n");
      return r;
    }
    
    return r;
}

int
yfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;

    lc->acquire(ino);
    std::string buf;
    r = ec->get(ino, buf);
    lc->release(ino);
    if (r != OK) {
      printf("read: file not exist\n");
      return r;
    }

    data = buf;

    return r;
}

int
yfs_client::symlink(inum parent, const char * name, const char * link, inum & ino_out)
{
    int r = OK;

    lc->acquire(parent);
    std::string buf;
    r = ec->get(parent, buf);
    if (r != OK) {
      printf("symlink: parent directory not exist\n");
      lc->release(parent);
      return r;
    }

    inum id;
    DirTable table(buf);
    if (table.lookup(std::string(name), id)) {
      printf("symlink: already exist\n");
      lc->release(parent);
      return EXIST;
    }

    r = ec->create(extent_protocol::T_SYMLINK, ino_out);
    if (r != OK) {
      printf("symlink: creation failure\n");
      lc->release(parent);
      return r;
    }

    r = ec->put(ino_out, std::string(link));
    if (r != OK) {
      printf("symlink: writing failed\n");
      lc->release(parent);
      return r;
    }
    
    table.insert(std::string(name), ino_out);
    r = ec->put(parent, table.dump());
    if (r != OK) {
      printf("symlink: parent directory update failed\n");
      lc->release(parent);
      return r;
    }

    lc->release(parent);
    return r;
}

void yfs_client::commit()
{
    ec->commit();
}

void yfs_client::undo()
{
    ec->undo();
}

void yfs_client::redo()
{
    ec->redo();
}

