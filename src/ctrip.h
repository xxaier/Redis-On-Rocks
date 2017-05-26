#ifndef SRC_CTRIP_H_
#define SRC_CTRIP_H_

#define XREDIS_VERSION "0.0.3"

void xslaveofCommand(redisClient *c);
void refullsyncCommand(redisClient *c);

#endif
