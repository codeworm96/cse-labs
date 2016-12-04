#include "inode_manager.h"
#include <cstring>
#include <ctime>
#include <pthread.h>

#define WRITE_AMPLIFICATION 4

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // use lock to ensure allocation is thread-safe
  pthread_mutex_lock(&bitmap_mutex);
  char buf[BLOCK_SIZE];
  blockid_t cur = 0;
  while (cur < sb.nblocks) {
    read_block(BBLOCK(cur), buf);
    for (int i = 0; i < BLOCK_SIZE && cur < sb.nblocks; ++i) {
      unsigned char mask = 0x80;
      while (mask > 0 && cur < sb.nblocks) {
        if ((buf[i] & mask) == 0) {
          buf[i] = buf[i] | mask;
          write_block(BBLOCK(cur), buf);
          pthread_mutex_unlock(&bitmap_mutex);
          return cur;
        }
        mask = mask >> 1;
        ++cur;
      }
    }
  }
  printf("\tim: error! out of blocks\n");
  pthread_mutex_unlock(&bitmap_mutex);
  exit(0);
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // use lock to ensure free is thread-safe
  pthread_mutex_lock(&bitmap_mutex);
  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id), buf);

  int index = (id % BPB) >> 3;
  unsigned char mask = 0xFF ^ (1 << (7 - ((id % BPB) & 0x7)));
  buf[index] = buf[index] & mask;

  write_block(BBLOCK(id), buf);
  pthread_mutex_unlock(&bitmap_mutex);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM / WRITE_AMPLIFICATION;
  sb.nblocks = BLOCK_NUM / WRITE_AMPLIFICATION;
  sb.ninodes = INODE_NUM;
  sb.version = 0;
  sb.next_inode = 1;
  sb.inode_end = 1;

  /* mark bootblock, superblock, bitmap, inode table region as used */
  char buf[BLOCK_SIZE];
  blockid_t cur = 0;
  blockid_t ending = RESERVED_BLOCK(sb.ninodes, sb.nblocks);
  while (cur < ending) {
    read_block(BBLOCK(cur), buf);
    for (int i = 0; i < BLOCK_SIZE && cur < ending; ++i) {
      unsigned char mask = 0x80;
      while (mask > 0 && cur < ending) {
        buf[i] = buf[i] | mask;
        mask = mask >> 1;
        ++cur;
      }
    }
    write_block(BBLOCK(cur - 1), buf);
  }

  bzero(buf, sizeof(buf));
  std::memcpy(buf, &sb, sizeof(sb));
  write_block(1, buf);

  pthread_mutex_init(&bitmap_mutex, NULL);
}

inline char parity(char x)
{
    char res = x;
    res = res ^ (res >> 4);
    res = res ^ (res >> 2);
    res = res ^ (res >> 1);
    return res & 1;
}

inline char encode84(char x) 
{
    char res = x & 0x07;
    res |= (x & 0x08) << 1; 
    res |= parity(res & 0x15) << 6;
    res |= parity(res & 0x13) << 5;
    res |= parity(res & 0x07) << 3;
    res |= parity(res) << 7;
    return res;
}

inline bool decode84(char x, char & res) 
{
    bool syndrome = false;
    char tmp = x;
    int fix = 0;
    if (parity(tmp & 0x55)) fix += 1;
    if (parity(tmp & 0x33)) fix += 2;
    if (parity(tmp & 0x0F)) fix += 4;
    if (fix) {
        syndrome = true;
        tmp ^= 1 << (7 - fix);
    }

    if (syndrome && !parity(x)) {
        return false;
    }

    res = tmp & 0x07;
    res |= (tmp & 0x10) >> 1; 
    return true;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
    char code[BLOCK_SIZE * WRITE_AMPLIFICATION];
    d->read_block(id * WRITE_AMPLIFICATION, code);
    d->read_block(id * WRITE_AMPLIFICATION + 1, code + BLOCK_SIZE);
    d->read_block(id * WRITE_AMPLIFICATION + 2, code + BLOCK_SIZE * 2);
    d->read_block(id * WRITE_AMPLIFICATION + 3, code + BLOCK_SIZE * 3);
    for (int i = 0; i < BLOCK_SIZE; ++i) {
        char low, high;
        if (!decode84(code[i * 2], low)) {
            if (!decode84(code[BLOCK_SIZE * 2 + i * 2], low)) {
                printf("\tim: error! error can not be corrected\n");
                // exit(1);
            }
        }
        if (!decode84(code[i * 2 + 1], high)) {
            if (!decode84(code[BLOCK_SIZE * 2 + i * 2 + 1], high)) {
                printf("\tim: error! error can not be corrected\n");
                // exit(1);
            }
        }
        buf[i] = (high << 4) | low;
    }
    write_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
    char code[BLOCK_SIZE * WRITE_AMPLIFICATION];
    for (int i = 0; i < BLOCK_SIZE; ++i) {
        char low = encode84(buf[i] & 0x0F);
        char high = encode84((buf[i] >> 4) & 0x0F);
        code[i * 2] = low;
        code[i * 2 + 1] = high;
        code[BLOCK_SIZE * 2 + i * 2] = low;
        code[BLOCK_SIZE * 2 + i * 2 + 1] = high;
    }
    d->write_block(id * WRITE_AMPLIFICATION, code);
    d->write_block(id * WRITE_AMPLIFICATION + 1, code + BLOCK_SIZE);
    d->write_block(id * WRITE_AMPLIFICATION + 2, code + BLOCK_SIZE * 2);
    d->write_block(id * WRITE_AMPLIFICATION + 3, code + BLOCK_SIZE * 3);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  pthread_mutex_init(&inodes_mutex, NULL);
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  // use lock to ensure allocation is thread-safe
  unsigned int inum;
  unsigned int pos;
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inodes_mutex);
  inum = bm->sb.next_inode++;
  pos = bm->sb.inode_end++;
  bzero(buf, sizeof(buf));
  std::memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inodes_mutex);

  bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf;
  ino->commit = -1;
  ino->type = type;
  ino->inum = inum;
  ino->pos = pos;
  ino->size = 0;
  ino->atime = std::time(0);
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
  
  return inum;
}

void
inode_manager::commit()
{
  unsigned int pos;
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inodes_mutex);
  pos = bm->sb.inode_end++;

  bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf;
  ino->commit = bm->sb.version++;
  bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
  
  bzero(buf, sizeof(buf));
  std::memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inodes_mutex);
}

void
inode_manager::undo()
{
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inodes_mutex);
  --bm->sb.version;
  while (true) {
      bm->read_block(IBLOCK(--bm->sb.inode_end, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->commit == (short)bm->sb.version) {
          bzero(buf, sizeof(buf));
          std::memcpy(buf, &bm->sb, sizeof(bm->sb));
          bm->write_block(1, buf);
          pthread_mutex_unlock(&inodes_mutex);
          return;
      }
  }
}

void
inode_manager::redo()
{
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inodes_mutex);
  while (true) {
      bm->read_block(IBLOCK(bm->sb.inode_end++, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->commit == (short)bm->sb.version) {
          bm->sb.version++;
          bzero(buf, sizeof(buf));
          std::memcpy(buf, &bm->sb, sizeof(bm->sb));
          bm->write_block(1, buf);
          pthread_mutex_unlock(&inodes_mutex);
          return;
      }
  }
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk, *res;
  unsigned int pos;
  char buf[BLOCK_SIZE];
  char buf2[BLOCK_SIZE];
  char buf3[BLOCK_SIZE];
  char new_indirect[BLOCK_SIZE];
  char old_indirect[BLOCK_SIZE];
  bool old = false;
  pthread_mutex_lock(&inodes_mutex);
  pos = bm->sb.inode_end - 1;
  while (pos > 0) {
      bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
      ino = (inode_t *)buf;
      if (ino->commit != -1) {
          old = true;
      } else if (ino->inum == inum) {
          if (ino->type == 0) {
              return NULL;
          }
          break;
      }
      --pos;
  }

  if (old) {
      pos = bm->sb.inode_end++;
      bzero(buf, sizeof(buf2));
      std::memcpy(buf, &bm->sb, sizeof(bm->sb));
      bm->write_block(1, buf2);

      bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf2);
      ino_disk = (inode_t *)buf2;
      ino_disk->commit = ino->commit;
      ino_disk->type = ino->type;
      ino_disk->inum = ino->inum;
      ino_disk->pos = pos;
      ino_disk->size = ino->size;
      ino_disk->atime = ino->atime;
      ino_disk->mtime = ino->mtime;
      ino_disk->ctime = ino->ctime;
      unsigned int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
      if (block_num > NDIRECT) {
        bm->read_block(ino->blocks[NDIRECT], old_indirect);
        for (unsigned int i = NDIRECT; i < block_num; ++i) {
            bm->read_block(old_indirect[i - NDIRECT], buf3);
            new_indirect[i - NDIRECT] = bm->alloc_block();
            bm->write_block(new_indirect[i - NDIRECT], buf3);
        }
        ino_disk->blocks[NDIRECT] = bm->alloc_block();
        bm->write_block(ino_disk->blocks[NDIRECT], new_indirect);
      }
      for (unsigned int i = 0; i < MIN(block_num, NDIRECT); ++i) {
          bm->read_block(ino->blocks[i], buf3);
          ino_disk->blocks[i] = bm->alloc_block();
          bm->write_block(ino_disk->blocks[i], buf3);
      }

      bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
      ino = ino_disk;
  }

  res = (struct inode*)malloc(sizeof(*res));
  *res = *ino;
  pthread_mutex_unlock(&inodes_mutex);
  return res;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(ino->pos, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(ino->pos, bm->sb.nblocks), buf);
}


/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  char block[BLOCK_SIZE];
  inode_t * ino = get_inode(inum);
  char * buf = (char *)malloc(ino->size);
  unsigned int cur = 0;
  for (int i = 0; i < NDIRECT && cur < ino->size; ++i) {
    if (ino->size - cur > BLOCK_SIZE) {
      bm->read_block(ino->blocks[i], buf + cur);
      cur += BLOCK_SIZE;
    } else {
      int len = ino->size - cur;
      bm->read_block(ino->blocks[i], block);
      memcpy(buf + cur, block, len);
      cur += len;
    }
  }

  if (cur < ino->size) {
    char indirect[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < NINDIRECT && cur < ino->size; ++i) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (ino->size - cur > BLOCK_SIZE) {
        bm->read_block(ix, buf + cur);
        cur += BLOCK_SIZE;
      } else {
        int len = ino->size - cur;
        bm->read_block(ix, block);
        memcpy(buf + cur, block, len);
        cur += len;
      }
    }
  }

  *buf_out = buf;
  *size = ino->size;
  ino->atime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  char block[BLOCK_SIZE];
  char indirect[BLOCK_SIZE];
  inode_t * ino = get_inode(inum);
  unsigned int old_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  /* free some blocks */
  if (old_block_num > new_block_num) {
    if (new_block_num > NDIRECT) {
      bm->read_block(ino->blocks[NDIRECT], indirect);
      for (unsigned int i = new_block_num; i < old_block_num; ++i) {
        bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
      }
    } else {
      if (old_block_num > NDIRECT) {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = NDIRECT; i < old_block_num; ++i) {
          bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
        }
        bm->free_block(ino->blocks[NDIRECT]);
        for (unsigned int i = new_block_num; i < NDIRECT; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      } else {
        for (unsigned int i = new_block_num; i < old_block_num; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      }
    }
  }

  /* new some blocks */
  if (new_block_num > old_block_num) {
    if (new_block_num <= NDIRECT) {
      for (unsigned int i = old_block_num; i < new_block_num; ++i) {
        ino->blocks[i] = bm->alloc_block();
      }
    } else {
      if (old_block_num <= NDIRECT) {
        for (unsigned int i = old_block_num; i < NDIRECT; ++i) {
          ino->blocks[i] = bm->alloc_block();
        }
        ino->blocks[NDIRECT] = bm->alloc_block();

        bzero(indirect, BLOCK_SIZE);
        for (unsigned int i = NDIRECT; i < new_block_num; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      } else {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = old_block_num; i < new_block_num; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      }
    }
  }

  /* write file content */

  int cur = 0;
  for (int i = 0; i < NDIRECT && cur < size; ++i) {
    if (size - cur > BLOCK_SIZE) {
      bm->write_block(ino->blocks[i], buf + cur);
      cur += BLOCK_SIZE;
    } else {
      int len = size - cur;
      memcpy(block, buf + cur, len);
      bm->write_block(ino->blocks[i], block);
      cur += len;
    }
  }

  if (cur < size) {
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < NINDIRECT && cur < size; ++i) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (size - cur > BLOCK_SIZE) {
        bm->write_block(ix, buf + cur);
        cur += BLOCK_SIZE;
      } else {
        int len = size - cur;
        memcpy(block, buf + cur, len);
        bm->write_block(ix, block);
        cur += len;
      }
    }
  }

  /* update inode */
  ino->size = size;
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t * ino = get_inode(inum);

  if (ino) {
    a.type = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size = ino->size;

    free(ino);
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  unsigned int pos;
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inodes_mutex);
  pos = bm->sb.inode_end++;

  bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf;
  ino->commit = -1;
  ino->type = 0;
  ino->inum = inum;
  bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
  
  bzero(buf, sizeof(buf));
  std::memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inodes_mutex);
}
