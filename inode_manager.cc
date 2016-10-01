#include "inode_manager.h"
#include <cstring>
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
  if (id < 0 || id >= BLOCK_NUM || buf == NULL) {
    return;
  }

  std::memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
  if (id < 0 || id >= BLOCK_NUM || buf == NULL) {
    return;
  }

  std::memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */
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
          return cur;
        }
        mask = mask >> 1;
        ++cur;
      }
    }
  }
  return cur;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id), buf);

  int index = (id % BPB) >> 3;
  unsigned char mask = 0xFF ^ (1 << (7 - (index & 0x7)));
  buf[index] = buf[index] & mask;

  write_block(BBLOCK(id), buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

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
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
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
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
    
   * if you get some heap memory, do not forget to free it.
   */
  char buf[BLOCK_SIZE];
  uint32_t cur = 1;
  while (cur <= bm->sb.ninodes) {
    bm->read_block(IBLOCK(cur, bm->sb.nblocks), buf);
    for (int i = 0; i < IPB && cur <= bm->sb.ninodes; ++i) {
      inode_t * ino = (inode_t *)(buf + i * sizeof(inode_t));
      if (ino->type == 0) {
        ino->type = type;
        ino->size = 0;
        ino->atime = std::time(0);
        ino->mtime = std::time(0);
        ino->ctime = std::time(0);
        bm->write_block(IBLOCK(cur, bm->sb.nblocks), buf);
        return cur;
      }
      ++cur;
    }
  }
  return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode * ino = get_inode(inum);

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
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
}
