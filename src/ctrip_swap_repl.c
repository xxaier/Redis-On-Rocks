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

/* See replicationCacheMaster for more details */
void replicationCacheSwapDrainingMaster(client *c) {
    serverAssert(server.swap_draining_master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected swap draining master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.swap_draining_master->querybuf);
    sdsclear(server.swap_draining_master->pending_querybuf);
    server.swap_draining_master->read_reploff = server.swap_draining_master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.swap_draining_master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

void replClientDiscardSwappingState(client *c) {
    listNode *ln;

    ln = listSearchKey(server.repl_swapping_clients, c);
    if (ln == NULL) return;

    listDelNode(server.repl_swapping_clients,ln);
    c->flags &= ~CLIENT_SWAPPING;
    serverLog(LL_NOTICE, "discarded: swapping repl client (reploff=%lld, read_reploff=%lld)", c->reploff, c->read_reploff);
}

static void replClientUpdateSelectedDb(client *c) {
    int dbid = -1;
    long long value;

    if (c->flags & CLIENT_MULTI) {
        for (int i = 0; i < c->mstate.count; i++) {
            if (c->mstate.commands[i].cmd->proc == selectCommand) {
                if (getLongLongFromObject(c->mstate.commands[i].argv[1],
                            &value) == C_OK) {
                    /* The last select in multi will take effect. */
                    dbid = value;
                }
            }
        }
    } else {
        if (c->cmd->proc == selectCommand) {
            if (getLongLongFromObject(c->argv[1],&value) == C_OK) {
                dbid = value;
            }
        }
    }

    if (dbid < 0) {
        /* repl client db not updated */
    } else if (dbid >= server.dbnum) {
        serverLog(LL_WARNING,"repl client select db out of range %d",dbid);
    } else {
        selectDb(c,dbid);
    }
}

/* Move command from repl client to repl worker client. */
static void replCommandDispatch(client *wc, client *c) {
    if (wc->argv) zfree(wc->argv);

    wc->db = c->db;

    /* master client selected db are pass to worker clients when dispatch,
     * so we need to keep track of the selected db as if commands are
     * executed by master clients instantly. */
    replClientUpdateSelectedDb(c);

    wc->argc = c->argc, c->argc = 0;
    wc->argv = c->argv, c->argv = NULL;
    wc->cmd = c->cmd;
    wc->lastcmd = c->lastcmd;
    wc->flags = c->flags;
    wc->cmd_reploff = c->cmd_reploff;
    wc->repl_client = c;

    /* Move repl client mstate to worker client if needed. */
    if (c->flags & CLIENT_MULTI) {
        c->flags &= ~CLIENT_MULTI;
        wc->mstate = c->mstate;
        initClientMultiState(c);
    }

    /* keyrequest_count is dispatched command count. Note that free repl
     * client would be defered untill swapping count drops to 0. */
    c->keyrequests_count++;
}

static void processFinishedReplCommands() {
    listNode *ln;
    client *wc, *c;
    struct redisCommand *backup_cmd;

    serverLog(LL_DEBUG, "> processFinishedReplCommands");

    while ((ln = listFirst(server.repl_worker_clients_used))) {
        wc = listNodeValue(ln);
        if (wc->CLIENT_REPL_SWAPPING) break;
        c = wc->repl_client;

        wc->flags &= ~CLIENT_SWAPPING;
        c->keyrequests_count--;
        listDelNode(server.repl_worker_clients_used, ln);
        listAddNodeTail(server.repl_worker_clients_free, wc);

        serverAssert(c->flags&CLIENT_MASTER);

        backup_cmd = c->cmd;
        c->cmd = wc->cmd;
        server.current_client = c;

        if (wc->swap_errcode) {
            rejectCommandFormat(c,"Swap failed (code=%d)",wc->swap_errcode);
            wc->swap_errcode = 0;
        } else {
            call(wc, CMD_CALL_FULL);

            /* post call */
            c->woff = server.master_repl_offset;
            if (listLength(server.ready_keys))
                handleClientsBlockedOnKeys();
        }

        c->cmd = backup_cmd;

        commandProcessed(wc);

        serverAssert(wc->client_hold_mode == CLIENT_HOLD_MODE_REPL);

        long long prev_offset = c->reploff;
        /* update reploff */
        if (c->flags&CLIENT_MASTER) {
            /* transaction commands wont dispatch to worker client untill
             * exec (queued by repl client), so worker client wont have
             * CLIENT_MULTI flag after call(). */
            serverAssert(!(wc->flags & CLIENT_MULTI));
            /* Update the applied replication offset of our master. */
            c->reploff = wc->cmd_reploff;
        }

		/* If the client is a master we need to compute the difference
		 * between the applied offset before and after processing the buffer,
		 * to understand how much of the replication stream was actually
		 * applied to the master state: this quantity, and its corresponding
		 * part of the replication stream, will be propagated to the
		 * sub-replicas and to the replication backlog. */
		if ((c->flags&CLIENT_MASTER)) {
			size_t applied = c->reploff - prev_offset;
			if (applied) {
				if(!server.repl_slave_repl_all){
					replicationFeedSlavesFromMasterStream(server.slaves,
							c->pending_querybuf, applied);
				}
				sdsrange(c->pending_querybuf,applied,-1);
			}
		}

        clientReleaseLocks(wc,NULL/*ctx unused*/);
    }
    serverLog(LL_DEBUG, "< processFinishedReplCommands");
}

void replWorkerClientKeyRequestFinished(client *wc, swapCtx *ctx) {
    client *c;
    listNode *ln;
    list *repl_swapping_clients;
    UNUSED(ctx);

    serverLog(LL_DEBUG, "> replWorkerClientSwapFinished client(id=%ld,cmd=%s,key=%s)",
        wc->id,wc->cmd->name,wc->argc <= 1 ? "": (sds)wc->argv[1]->ptr);

    DEBUG_MSGS_APPEND(&ctx->msgs, "request-finished", "errcode=%d",ctx->errcode);

    if (ctx->errcode) clientSwapError(wc,ctx->errcode);
    keyRequestBeforeCall(wc,ctx);

    /* Flag swap finished, note that command processing will be defered to
     * processFinishedReplCommands becasue there might be unfinished preceeding swap. */
    wc->keyrequests_count--;
    swapCmdSwapFinished(ctx->key_request->swap_cmd);
    if (wc->keyrequests_count == 0) wc->CLIENT_REPL_SWAPPING = 0;

    processFinishedReplCommands();

    /* Dispatch repl command again for repl client blocked waiting free
     * worker repl client, because repl client might already read repl requests
     * into querybuf, read event will not trigger if we do not parse and
     * process again.  */
    if (!listFirst(server.repl_swapping_clients) ||
            !listFirst(server.repl_worker_clients_free)) {
        serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
        return;
    }

    repl_swapping_clients = server.repl_swapping_clients;
    server.repl_swapping_clients = listCreate();
    while ((ln = listFirst(repl_swapping_clients))) {
        int swap_result;

        c = listNodeValue(ln);
        /* Swapping repl clients are bound to:
         * - have pending parsed but not processed commands
         * - in server.repl_swapping_client list
         * - flag have CLIENT_SWAPPING */
        serverAssert(c->argc);
        serverAssert(c->flags & CLIENT_SWAPPING);

        /* Must make sure swapping clients satistity above constrains. also
         * note that repl client never call(only dispatch). */
        c->flags &= ~CLIENT_SWAPPING;
        swap_result = submitReplClientRequests(c);
        /* replClientSwap return 1 on dispatch fail, -1 on dispatch success,
         * never return 0. */
        if (swap_result > 0) {
            c->flags |= CLIENT_SWAPPING;
        } else {
            commandProcessed(c);
        }

        /* TODO confirm whether server.current_client == NULL possible */
        processInputBuffer(c);

        listDelNode(repl_swapping_clients,ln);
    }
    listRelease(repl_swapping_clients);

    serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
}

int submitReplWorkerClientRequest(client *wc) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(wc, &result);
    wc->keyrequests_count = result.num;
    submitClientKeyRequests(wc,&result,replWorkerClientKeyRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}

/* Different from original replication stream process, slave.master client
 * might trigger swap and block untill rocksdb IO finish. because there is
 * only one master client so rocksdb IO will be done sequentially, thus slave
 * can't catch up with master.
 * In order to speed up replication stream processing, slave.master client
 * dispatches command to multiple worker client and execute commands when
 * rocks IO finishes. Note that replicated commands swap in-parallel but
 * processed in received order. */
int submitReplClientRequests(client *c) {
    client *wc;
    listNode *ln;

    c->cmd_reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    serverAssert(!(c->flags & CLIENT_SWAPPING));
    if (!(ln = listFirst(server.repl_worker_clients_free))) {
        /* return swapping if there are no worker to dispatch, so command
         * processing loop would break out.
         * Note that peer client might register no rocks callback but repl
         * stream read and parsed, we need to processInputBuffer again. */
        listAddNodeTail(server.repl_swapping_clients, c);
        /* Note repl client will be flagged CLIENT_SWAPPING when return. */
        return 1;
    }

    wc = listNodeValue(ln);
    serverAssert(wc && !wc->CLIENT_REPL_SWAPPING);

    /* Because c is a repl client, only normal multi {cmd} exec will be
     * received (multiple multi, exec without multi, ... will no happen) */
    if (c->cmd->proc == multiCommand) {
        serverAssert(!(c->flags & CLIENT_MULTI));
        c->flags |= CLIENT_MULTI;
    } else if (c->flags & CLIENT_MULTI &&
            c->cmd->proc != execCommand &&
            !isGtidExecCommand(c)) {
        serverPanic("command should be already queued.");
    } else {
        /* either vanilla command or transaction are stored in client state,
         * client is ready to dispatch now. */
        replCommandDispatch(wc, c);

        /* swap data for replicated commands, note that command will be
         * processed later in processFinishedReplCommands untill all preceeding
         * commands finished. */
        submitReplWorkerClientRequest(wc);
        wc->CLIENT_REPL_SWAPPING = wc->keyrequests_count;

        listDelNode(server.repl_worker_clients_free, ln);
        listAddNodeTail(server.repl_worker_clients_used, wc);
    }

    /* process repl commands in received order (not swap finished order) so
     * that slave is consistent with master. */
    processFinishedReplCommands();

    /* return dispatched(-1) when repl dispatched command to workers, caller
     * should skip call and continue command processing loop. */
    return -1;
}

sds genSwapReplInfoString(sds info) {
    info = sdscatprintf(info,
            "swap_repl_workers:free=%lu,used=%lu,swapping=%lu\r\n",
            listLength(server.repl_worker_clients_free),
            listLength(server.repl_worker_clients_used),
            listLength(server.repl_swapping_clients));
    return info;
}
