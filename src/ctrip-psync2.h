/**
 * if merge with redis new version, just delete ctrip-psync2.h and ctrip-psync2.c
 */
#ifndef SRC_CTRIP_PSYNC2_H_
#define SRC_CTRIP_PSYNC2_H_

void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen);
void replicationCacheMasterUsingMyself(void);
void feedReplicationBacklog(void *ptr, size_t len);
void addReplyString(redisClient *c, char *s, size_t len);

void changeReplicationId(void);
void clearReplicationId2(void);
void shiftReplicationId(void);

#endif /* SRC_CTRIP_PSYNC2_H_ */
