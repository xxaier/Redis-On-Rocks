#ifndef SRC_CTRIP_H_
#define SRC_CTRIP_H_

#define XREDIS_VERSION "0.0.4"
#define REDIS_DEFAULT_SLAVE_REPLICATE_ALL 0

void xslaveofCommand(redisClient *c);
void refullsyncCommand(redisClient *c);

#endif
