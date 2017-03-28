#ifndef CSUM_H
#define CSUM_H

static const unsigned int crctab[] = {
  0x00000000, 0x1db71064, 0x3b6e20c8, 0x26d930ac,
  0x76dc4190, 0x6b6b51f4, 0x4db26158, 0x5005713c,
  0xedb88320, 0xf00f9344, 0xd6d6a3e8, 0xcb61b38c,
  0x9b64c2b0, 0x86d3d2d4, 0xa00ae278, 0xbdbdf21c
};

static inline void
update_crc32(const unsigned char *data, unsigned int length, unsigned int *crc)
{
  const unsigned char *p = data;
  const unsigned int *crctabp = crctab;
  int i;

  i = length;
  while (i-- > 0) {
    *crc ^= *p++;
    *crc = (*crc >> 4) ^ crctabp[*crc & 0xf];
    *crc = (*crc >> 4) ^ crctabp[*crc & 0xf];
  }
}

#define INITIAL_CRC32_VALUE 0xFFFFFFFFU

#endif /* CSUM_H */
