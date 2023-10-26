/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"


list *clientRenewLocks(client *c) {
    list *old = c->swap_locks;
    c->swap_locks = listCreate();
    return old;
}

void clientGotLock(client *c, swapCtx *ctx, void *lock) {
    serverAssert(ctx->swap_lock == NULL);
    ctx->swap_lock = lock;
    switch (c->client_hold_mode) {
    case CLIENT_HOLD_MODE_CMD:
    case CLIENT_HOLD_MODE_REPL:
        serverAssert(c->swap_locks != NULL);
        listAddNodeTail(c->swap_locks,lock);
        break;
    case CLIENT_HOLD_MODE_EVICT:
    default:
        break;
    }
}

void clientReleaseRequestIO(client *c, swapCtx *ctx) {
    UNUSED(c);
    lockProceeded(ctx->swap_lock);
}

void clientReleaseLocks(client *c, swapCtx *ctx) {
    list *locks;
    listNode *ln;
    listIter li;

    switch (c->client_hold_mode) {
    case CLIENT_HOLD_MODE_CMD:
    case CLIENT_HOLD_MODE_REPL:
        locks = clientRenewLocks(c);
        listRewind(locks,&li);
        while ((ln = listNext(&li))) {
            lockUnlock(listNodeValue(ln));
        }
        listRelease(locks);
        break;
    case CLIENT_HOLD_MODE_EVICT:
        if (ctx->swap_lock) {
            lockUnlock(ctx->swap_lock);
        }
        break;
    default:
        break;
    }
}

/* Swap Rewind
 *
 * If write commands comes right after slaveof/failover command, it will be
 * waiting in lock queue (after processCommand). when slaveof/failover execute
 * finished, server will change state (slaveof command makes server readonly,
 * failover command makes server pause write), but those commands will not
 * go through processCommand to check server status again, resulting unexepcted
 * server status:
 * - slaveof command will execute those inflight commands, making it's
 *   replication history longer than other replica and will result in fullresync
 *   if try to sync with other replica.
 * - failover command will execute those inflight commands, making it not
 *   effectly paused, thus it's data will be inconsist with the new master.
 *
 * We introduce swap rewind feature to address these issue: if any client is
 * about to change server status, we register upcomming clients and rewind
 * those clients to go through processCommand again.
 *
 * Note that:
 *  - master client should not rewind: master worker client are kept in order,
 *   rewind might mess with it's order.
 *  - evict/expire client currently dont need to rewind, because those client
 *   did not go through processCommands.
 */

void startSwapRewind(swap_rewind_type rewind_type) {
    server.swap_rewind_type = rewind_type;
    serverAssert(rewind_type != SWAP_REWIND_OFF);
    serverLog(LL_WARNING,"Start swap rewind(%d), current master_repl_offset:%lld", rewind_type, server.master_repl_offset);
}

static void processSwapRewindingClients(void) {
    listNode *ln;
    while (listLength(server.swap_rewinding_clients)) {
        ln = listFirst(server.swap_rewinding_clients);
        serverAssert(ln != NULL);
        client *c = listNodeValue(ln);
        listDelNode(server.swap_rewinding_clients,ln);
        c->flags &= ~CLIENT_SWAP_REWINDING;
        queueClientForReprocessing(c);
    }
}

void endSwapRewind() {
    server.swap_rewind_type = SWAP_REWIND_OFF;
    listJoin(server.swap_rewinding_clients,server.swap_torewind_clients);
    processSwapRewindingClients();
    serverLog(LL_WARNING,"End swap rewind, current master_repl_offset:%lld", server.master_repl_offset);
}

static void startSwapRewindIfNeeded(client *c) {
    if (c->cmd && (c->cmd->proc == failoverCommand ||
                c->cmd->proc == replicaofCommand)) {
        startSwapRewind(SWAP_REWIND_WRITE);
    }
}

static void registerSwapToRewindClient(client *c) {
    serverAssert(c->cmd);
    c->flags |= CLIENT_SWAP_REWINDING;
    listAddNodeTail(server.swap_torewind_clients,c);
}

static int registerSwapToRewindClientIfNeeded(client *c) {
    int is_may_replicate_command = (c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE)) ||
                                   (c->cmd->proc == execCommand && (c->mstate.cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)));
    if (!(c->flags & CLIENT_SLAVE) &&
        ((server.swap_rewind_type == SWAP_REWIND_ALL) ||
        (server.swap_rewind_type == SWAP_REWIND_WRITE && is_may_replicate_command))) {
        registerSwapToRewindClient(c);
        return 1;
    } else {
        return 0;
    }
}

/* SwapCtx manages context and data for swapping specific key. Note that:
 * - key_request copy to swapCtx.key_request
 * - swapdata moved to swapCtx,
 * - swapRequest managed by async/sync complete queue (not by swapCtx).
 * swapCtx released when keyRequest finishes. */
swapCtx *swapCtxCreate(client *c, keyRequest *key_request,
        clientKeyRequestFinished finished, void* pd) {
    swapCtx *ctx = zcalloc(sizeof(swapCtx));
    ctx->c = c;
    moveKeyRequest(ctx->key_request,key_request);
    ctx->finished = finished;
    ctx->errcode = 0;
    ctx->swap_lock = NULL;
#ifdef SWAP_DEBUG
    char *key = key_request->key ? key_request->key->ptr : "(nil)";
    char identity[MAX_MSG];
    snprintf(identity,MAX_MSG,"[%s(%u):%s:%.*s]",
            swapIntentionName(key_request->cmd_intention),
            key_request->cmd_intention_flags,
            c->cmd->name,MAX_MSG/2,key);
    swapDebugMsgsInit(&ctx->msgs, identity);
#endif
    ctx->pd = pd;
    return ctx;
}

/* Bind data & datactx to swapCtx so that it will be properly cleaned up */
void swapCtxSetSwapData(swapCtx *ctx, MOVE swapData *data, MOVE void *datactx) {
    ctx->data = data;
    ctx->datactx = datactx;
}

void swapCtxFree(swapCtx *ctx) {
    if (!ctx) return;
#ifdef SWAP_DEBUG
    swapDebugMsgsDump(&ctx->msgs);
#endif
    keyRequestDeinit(ctx->key_request);
    if (ctx->data) {
        swapDataFree(ctx->data,ctx->datactx);
        ctx->data = NULL;
    }
    zfree(ctx);
}

void replySwapFailed(client *c) {
    serverAssert(c->swap_errcode);
    switch (c->swap_errcode) {
    case SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI:
        rejectCommandFormat(c,
                "Swap failed: scan not supported in multi.");
        break;
    case SWAP_ERR_METASCAN_SESSION_UNASSIGNED:
        rejectCommandFormat(c,
                "Swap failed: scan session unassigned");
        break;
    case SWAP_ERR_METASCAN_SESSION_INPROGRESS:
        rejectCommandFormat(c,
                "Swap failed: scan in progress.");
        break;
    case SWAP_ERR_METASCAN_SESSION_SEQUNMATCH:
        rejectCommandFormat(c,
                "Swap failed: cursor not match (restart scan with cursor 0 when failed)");
        break;
    case SWAP_ERR_DATA_WRONG_TYPE_ERROR:
        addReplyErrorObject(c,shared.wrongtypeerr);
        break;
    default:
        rejectCommandFormat(c,"Swap failed (code=%d)",c->swap_errcode);
        break;
    }
}

void continueProcessCommand(client *c) {
	c->flags &= ~CLIENT_SWAPPING;
    server.current_client = c;

	if (c->swap_errcode) {
        replySwapFailed(c);
        c->swap_errcode = 0;
    } else {
		call(c,CMD_CALL_FULL);
		/* post call */
		c->woff = server.master_repl_offset;
		if (listLength(server.ready_keys))
			handleClientsBlockedOnKeys();
	}

    /* unhold keys for current command. */
    serverAssert(c->client_hold_mode == CLIENT_HOLD_MODE_CMD);
    /* post command */
    commandProcessed(c);
    c->flags |= CLIENT_SWAP_UNLOCKING;
    clientReleaseLocks(c,NULL/*ctx unused*/);
    c->flags &= ~CLIENT_SWAP_UNLOCKING;

    /* pipelined command might already read into querybuf, if process not
     * restarted, pending commands would not be processed again. */
    if (!c->CLIENT_DEFERED_CLOSING) processInputBuffer(c);
}

void keyRequestBeforeCall(client *c, swapCtx *ctx) {
    swapData *data = ctx->data;
    void *datactx = ctx->datactx;
    if (data == NULL) return;
    if (!swapDataAlreadySetup(data)) return;
    swapDataBeforeCall(data,c,datactx);
}

void normalClientKeyRequestFinished(client *c, swapCtx *ctx) {
    robj *key = ctx->key_request->key;
    UNUSED(key);
    DEBUG_MSGS_APPEND(&ctx->msgs,"request-finished",
            "key=%s, keyrequests_count=%d, errcode=%d",
            key?(sds)key->ptr:"<nil>", c->keyrequests_count, ctx->errcode);
    c->keyrequests_count--;
    swapCmdSwapFinished(ctx->key_request->swap_cmd);
    if (ctx->errcode) clientSwapError(c,ctx->errcode);
    keyRequestBeforeCall(c,ctx);
    if (c->keyrequests_count == 0) {
        continueProcessCommand(c);
    }
}

void keyRequestSwapFinished(swapData *data, void *pd, int errcode) {
    UNUSED(data);
    swapCtx *ctx = pd;
	if (errcode) ctx->errcode = errcode;

    if (data) {
        swapDataKeyRequestFinished(data);
        DEBUG_MSGS_APPEND(&ctx->msgs,"swap-finished",
                "key=%s,propagate_expire=%d,set_dirty=%d",
                (sds)data->key->ptr,data->propagate_expire,data->set_dirty);
    }

    /* release io will trigger either another swap within the same tx or
     * command call, but never both. so swap and main thread will not
     * touch the same key in parallel. */
    clientReleaseRequestIO(ctx->c,ctx);

    ctx->finished(ctx->c,ctx);
}

/* Expired key should delete only if server is master, check expireIfNeeded
 * for more details. */
int keyExpiredAndShouldDelete(redisDb *db, robj *key) {
    if (!keyIsExpired(db,key)) return 0;
    if (server.masterhost != NULL) return 0;
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;
    return 1;
}

#define NOSWAP_REASON_KEYNOTEXISTS 1
#define NOSWAP_REASON_NOTKEYLEVEL 2
#define NOSWAP_REASON_KEYNOTSUPPORT 3
#define NOSWAP_REASON_SWAPANADECIDED 4
#define NOSWAP_REASON_FILT_BY_CUCKOOFILTER 5
#define NOSWAP_REASON_FILT_BY_ABSENTCACHE 6
#define NOSWAP_REASON_ALREAY_SWAPPED_OUT 7
#define NOSWAP_REASON_UNEXPECTED 100

void keyRequestProceed(void *lock, int flush, redisDb *db, robj *key,
        client *c, void *pd) {
    int reason_num = 0, retval = 0, swap_intention, errcode;
    void *datactx = NULL;
    swapData *data = NULL;
    swapCtx *ctx = pd;
    robj *value, *dirty_subkeys;
    objectMeta *object_meta;
    char *reason;
    void *msgs = NULL;
    uint32_t swap_intention_flags;
    long long expire;
    int cmd_intention = ctx->key_request->cmd_intention;
    uint32_t cmd_intention_flags = ctx->key_request->cmd_intention_flags;
    int thread_idx = ctx->key_request->deferred ? server.swap_defer_thread_idx : -1;
    UNUSED(reason);
    swapRequest *req = NULL;

#ifdef SWAP_DEBUG
    msgs = &ctx->msgs;
#endif

    serverAssert(c == ctx->c);
    clientGotLock(c,ctx,lock);

    if (db == NULL || key == NULL) {
        reason = "noswap needed for db/svr level request";
        reason_num = NOSWAP_REASON_NOTKEYLEVEL;
        goto noswap;
    }

	/* handle metascan request. */
    if (isMetaScanRequest(cmd_intention_flags)) {
        data = createSwapData(db,NULL,NULL,NULL);
        retval = swapDataSetupMetaScan(data,cmd_intention_flags,c,&datactx);
        swapCtxSetSwapData(ctx,data,datactx);
        if (retval) {
            ctx->errcode = retval;
            reason = "setup metascan failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
            goto noswap;
        } else {
            goto allset;
        }
    }

    value = lookupKey(db,key,LOOKUP_NOTOUCH);
    dirty_subkeys = lookupDirtySubkeys(db,key);

    data = createSwapData(db,key,value,dirty_subkeys);
    swapCtxSetSwapData(ctx,data,datactx);

    if (isSwapHitStatKeyRequest(ctx->key_request)) {
        atomicIncr(server.swap_hit_stats->stat_swapin_attempt_count,1);
    }

    /* slave expire decided before swap */
    if (cmd_intention_flags & SWAP_EXPIRE_FORCE) {
        swapDataMarkPropagateExpire(data);
    }

    if (value == NULL) {
        if (cmd_intention == SWAP_OUT) {
            /* nothing to persist or evict. */
            reason = "key already swapped out";
            reason_num = NOSWAP_REASON_ALREAY_SWAPPED_OUT;
            goto noswap;
        }

        int filt_by;
        if (!coldFilterMayContainKey(db->cold_filter,key->ptr,&filt_by)) {
            reason = "key is absent";
            if (filt_by == COLDFILTER_FILT_BY_CUCKOO_FILTER)
                reason_num = NOSWAP_REASON_FILT_BY_CUCKOOFILTER;
            else
                reason_num = NOSWAP_REASON_FILT_BY_ABSENTCACHE;
            goto noswap;
        } else {
            req = swapMetaRequestNew(ctx->key_request,
                    ctx,data,datactx,ctx->key_request->trace,
                    keyRequestSwapFinished,ctx,msgs);
            swapBatchCtxFeed(server.swap_batch_ctx,flush,req,thread_idx);
            return;
        }
    }

    expire = getExpire(db,key);

    retval = swapDataSetupMeta(data,value->type,expire,&datactx);
    swapCtxSetSwapData(ctx,data,datactx);
    if (retval) {
        if (retval == SWAP_ERR_SETUP_UNSUPPORTED) {
            reason = "data not support swap";
            reason_num = NOSWAP_REASON_KEYNOTSUPPORT;
        } else {
            ctx->errcode = retval;
            reason = "setup meta failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        }
        goto noswap;
    }

    object_meta = lookupMeta(db,key);
    swapDataSetObjectMeta(data,object_meta);

allset:

    if ((errcode = swapDataAna(data,SWAP_ANA_THD_MAIN,ctx->key_request,&swap_intention,
                &swap_intention_flags,datactx))) {
        if (errcode == SWAP_ERR_DATA_WRONG_TYPE_ERROR) {
            ctx->errcode = errcode;
            reason = "swap data type error";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        } else {
            ctx->errcode = SWAP_ERR_DATA_ANA_FAIL;
            reason = "swap ana failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        }

        goto noswap;
    }

    if (swap_intention == SWAP_NOP) {
        reason = "swapana decided no swap";
        reason_num = NOSWAP_REASON_SWAPANADECIDED;
        goto noswap;
    }

    DEBUG_MSGS_APPEND(&ctx->msgs,"request-proceed","start swap=%s",
            swapIntentionName(swap_intention));

    req = swapDataRequestNew(swap_intention,swap_intention_flags,ctx,data,
            datactx,ctx->key_request->trace,keyRequestSwapFinished,ctx,msgs);
    swapBatchCtxFeed(server.swap_batch_ctx,flush,req,thread_idx);

    return;

noswap:
    DEBUG_MSGS_APPEND(&ctx->msgs,"request-proceed",
            "no swap needed: %s", reason);
    if (isSwapHitStatKeyRequest(ctx->key_request)) {
        if (reason_num == NOSWAP_REASON_SWAPANADECIDED)
            atomicIncr(server.swap_hit_stats->stat_swapin_no_io_count,1);
        if (reason_num == NOSWAP_REASON_FILT_BY_CUCKOOFILTER)
            atomicIncr(server.swap_hit_stats->stat_swapin_not_found_coldfilter_cuckoofilter_filt_count,1);
        if (reason_num == NOSWAP_REASON_FILT_BY_ABSENTCACHE)
            atomicIncr(server.swap_hit_stats->stat_swapin_not_found_coldfilter_absentcache_filt_count,1);
    }

    /* noswap is kinda swapfinished. */
    if (ctx->key_request->trace) ctx->key_request->trace->swap_dispatch_time = getMonotonicUs();
    keyRequestSwapFinished(data,ctx,ctx->errcode);

    return;
}

void _submitClientKeyRequests(client *c, getKeyRequestsResult *result,
        clientKeyRequestFinished cb, void* ctx_pd, int deferred) {
    int64_t txid = server.swap_txid++;

    if (result->swap_cmd) swapCmdSwapSubmitted(result->swap_cmd);
    for (int i = 0; i < result->num; i++) {
        void *msgs = NULL;
        keyRequest *key_request = result->key_requests + i;
        key_request->deferred = deferred;
        redisDb *db = key_request->level == REQUEST_LEVEL_SVR ?
            NULL : server.db + key_request->dbid;
        robj *key = key_request->key;

        swapCtx *ctx = swapCtxCreate(c,key_request,cb, ctx_pd);
#ifdef SWAP_DEBUG
        msgs = &ctx->msgs;
#endif
        DEBUG_MSGS_APPEND(&ctx->msgs,"request-wait", "key=%s",
                key ? (sds)key->ptr : "<nil>");

        if (key_request->trace) swapTraceLock(key_request->trace);
        lockLock(txid,db,key,keyRequestProceed,c,ctx,
                (freefunc)swapCtxFree,msgs);
    }
}

void submitDeferredClientKeyRequests(client *c, getKeyRequestsResult *result,
                                     clientKeyRequestFinished cb, void* ctx_pd) {
    _submitClientKeyRequests(c, result, cb, ctx_pd, 1);
}

void submitClientKeyRequests(client *c, getKeyRequestsResult *result,
                             clientKeyRequestFinished cb, void* ctx_pd) {
    _submitClientKeyRequests(c, result, cb, ctx_pd, 0);
}

/* Returns submited keyrequest count, if any keyrequest submitted, command
 * gets called in contiunueProcessCommand instead of normal call(). */
int submitNormalClientRequests(client *c) {
    serverAssert(c->swap_cmd == NULL);
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(c,&result);
    c->keyrequests_count = result.num;
    if (registerSwapToRewindClientIfNeeded(c)) return result.num;
    startSwapRewindIfNeeded(c);
    submitClientKeyRequests(c,&result,normalClientKeyRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}

void swapMutexopCommand(client *c) {
    addReply(c, shared.ok);
}

int lockGlobalAndExec(clientKeyRequestFinished locked_op, uint64_t exclude_mark) {
    if (exclude_mark && server.req_submitted&exclude_mark) {
        return 0;
    }
    /* add flag before submit request otherwise when
     * global lock no block, flag may be del just after submit */
    server.req_submitted |= exclude_mark;

    client *c = server.mutex_client;
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequestsPrepareResult(&result,1);
    getKeyRequestsAppendSubkeyResult(&result,REQUEST_LEVEL_SVR,NULL,0,NULL,
                               c->cmd->intention,c->cmd->intention_flags,c->cmd->flags,c->db->id);
    submitClientKeyRequests(c,&result,locked_op,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return 1;
}

int dbSwap(client *c) {
    int keyrequests_submit;
    swapRatelimitCtx _rlctx = {0}, *rlctx = &_rlctx;

    swapRatelimitStart(rlctx,c);

    if (swapRateLimitReject(rlctx,c)) return C_OK;

    if (!(c->flags & CLIENT_MASTER)) {
        keyrequests_submit = submitNormalClientRequests(c);
    } else {
        keyrequests_submit = submitReplClientRequests(c);
    }

    swapRateLimitPause(rlctx,c);

    if (c->flags & CLIENT_SWAP_REWINDING) {
        /* Rewinding command parsed but not processed, See below */
        return C_ERR;
    } else if (keyrequests_submit > 0) {
        /* Swapping command parsed but not processed, return C_ERR so that:
         * 1. repl stream will not propagate to sub-slaves
         * 2. client will not reset
         * 3. client will break out process loop. */
        if (c->keyrequests_count) c->flags |= CLIENT_SWAPPING;
        return C_ERR;
    } else if (keyrequests_submit < 0) {
        /* Swapping command parsed and dispatched, return C_OK so that:
         * 1. repl client will skip call
         * 2. repl client will reset (cmd moved to worker).
         * 3. repl client will continue parse and dispatch cmd */
        return C_OK;
    } else {
        call(c,CMD_CALL_FULL);
        c->woff = server.master_repl_offset;
        if (listLength(server.ready_keys))
            handleClientsBlockedOnKeys();
        return C_OK;
    }

    return C_OK;
}

void swapInit() {
    int i;

    initStatsSwap();
    swapInitVersion();

    server.swap_eviction_ctx = swapEvictionCtxCreate();

    server.swap_load_inprogress_count = 0;

    server.evict_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("SWAP.EVICT");
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.evict_clients[i] = c;
    }

    server.load_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("SWAP.LOAD");
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.load_clients[i] = c;
    }

    server.expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("SWAP.EXPIRED");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.expire_clients[i] = c;
    }

    server.scan_expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("SWAP.SCANEXPIRE");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.scan_expire_clients[i] = c;
    }

    server.ttl_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("ttl");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.ttl_clients[i] = c;
    }

    server.mutex_client = createClient(NULL);
    server.mutex_client->cmd = lookupCommandByCString("SWAP.MUTEXOP");
    server.mutex_client->client_hold_mode = CLIENT_HOLD_MODE_EVICT;

    server.repl_workers = 256;
    server.repl_swapping_clients = listCreate();
    server.repl_worker_clients_free = listCreate();
    server.repl_worker_clients_used = listCreate();
    for (i = 0; i < server.repl_workers; i++) {
        client *c = createClient(NULL);
        c->client_hold_mode = CLIENT_HOLD_MODE_REPL;
        listAddNodeTail(server.repl_worker_clients_free, c);
    }

    server.rdb_load_ctx = NULL;

    swapLockCreate();

    server.swap_scan_sessions = swapScanSessionsCreate(server.swap_scan_session_bits);

    server.swap_dependency_block_ctx = createSwapUnblockCtx();

    for (i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        db->cold_filter = coldFilterCreate();
    }

    server.swap_batch_ctx = swapBatchCtxNew();

    if (server.swap_persist_enabled)
        server.swap_persist_ctx = swapPersistCtxNew();
    else
        server.swap_persist_ctx = NULL;
}



#ifdef REDIS_TEST

void initServerConfig(void);
void initServerConfig4Test(void) {
    static int inited;
    if (inited) return;
    initServerConfig();
    inited = 1;
}

int clearTestRedisDb() {
    emptyDbStructure(server.db, -1, 0, NULL);
    return 1;
}

int initTestRedisDb() {
    static int inited;
    if (inited) return 1;

    server.dbnum = 16;
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
    /* Create the Redis databases, and initialize other internal state. */
    for (int j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&dbExpiresDictType,NULL);
        server.db[j].meta = dictCreate(&objectMetaDictType, NULL);
        server.db[j].dirty_subkeys = dictCreate(&objectKeyPointerValueDictType, NULL);
        server.db[j].evict_asap = listCreate();
        server.db[j].cold_keys = 0;
        server.db[j].randomkey_nextseek = NULL;
        server.db[j].scan_expire = scanExpireCreate();
        server.db[j].cold_filter = coldFilterCreate(server.db+j);
        server.db[j].expires_cursor = 0;
        server.db[j].id = j;
        server.db[j].avg_ttl = 0;
        server.db[j].defrag_later = listCreate();
        listSetFreeMethod(server.db[j].defrag_later,(void (*)(void*))sdsfree);
    }

    inited = 1;
    return 1;
}

void createSharedObjects(void);
int initTestRedisServer() {
    server.maxmemory_policy = MAXMEMORY_FLAG_LFU;
    if (!server.logfile) server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    swapInitVersion();
    createSharedObjects();
    initTestRedisDb();
    return 1;
}
int clearTestRedisServer() {
    return 1;
}
int swapTest(int argc, char **argv, int accurate) {
  int result = 0;

  result += swapLockTest(argc, argv, accurate);
  result += swapLockReentrantTest(argc, argv, accurate);
  result += swapLockProceedTest(argc, argv, accurate);
  result += swapCmdTest(argc, argv, accurate);
  result += swapExecTest(argc, argv, accurate);
  result += swapDataTest(argc, argv, accurate);
  result += swapDataWholeKeyTest(argc, argv, accurate);
  result += swapObjectTest(argc, argv, accurate);
  result += swapRdbTest(argc, argv, accurate);
  result += swapIterTest(argc, argv, accurate);
  result += swapDataHashTest(argc, argv, accurate);
  result += swapDataSetTest(argc, argv, accurate);
  result += swapDataZsetTest(argc, argv, accurate);
  result += metaScanTest(argc, argv, accurate);
  result += swapExpireTest(argc, argv, accurate);
  result += swapUtilTest(argc, argv, accurate);
  result += swapFilterTest(argc, argv, accurate);
  result += swapListMetaTest(argc, argv, accurate);
  result += swapListDataTest(argc, argv, accurate);
  result += swapListUtilsTest(argc, argv, accurate);
  result += lruCacheTest(argc, argv, accurate);
  result += swapAbsentTest(argc, argv, accurate);
  result += swapRIOTest(argc, argv, accurate);
  result += swapBatchTest(argc, argv, accurate);
  result += cuckooFilterTest(argc, argv, accurate);
  result += swapPersistTest(argc, argv, accurate);

  return result;
}
#endif
