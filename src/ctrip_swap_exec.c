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

#define EXEC_OK      0
#define EXEC_FAIL    -1

#define RIO_SCAN_NUMKEYS_ALLOC_INIT 16
#define RIO_SCAN_NUMKEYS_ALLOC_LINER 4096

/* --- RIO --- */
void RIOInitGet(RIO *rio, int cf, sds rawkey) {
    rio->action = ROCKS_GET;
    rio->get.cf = cf;
    rio->get.rawkey = rawkey;
    rio->get.rawval = NULL;
    rio->err = NULL;
}

void RIOInitPut(RIO *rio, int cf, sds rawkey, sds rawval) {
    rio->action = ROCKS_PUT;
    rio->put.cf = cf;
    rio->put.rawkey = rawkey;
    rio->put.rawval = rawval;
    rio->err = NULL;
}

void RIOInitDel(RIO *rio, int cf, sds rawkey) {
    rio->action = ROCKS_DEL;
    rio->del.cf = cf;
    rio->del.rawkey = rawkey;
    rio->err = NULL;
}

void RIOInitWrite(RIO *rio, rocksdb_writebatch_t *wb) {
    rio->action = ROCKS_WRITE;
    rio->write.wb = wb;
    rio->err = NULL;
}

void RIOInitMultiGet(RIO *rio, int numkeys, int *cfs, sds *rawkeys) {
    rio->action = ROCKS_MULTIGET;
    rio->multiget.numkeys = numkeys;
    rio->multiget.cfs = cfs;
    rio->multiget.rawkeys = rawkeys;
    rio->multiget.rawvals = NULL;
    rio->err = NULL;
}

void RIOInitScan(RIO *rio, int cf, sds prefix) {
    rio->action = ROCKS_SCAN;
    rio->scan.cf = cf;
    rio->scan.prefix = prefix;
    rio->scan.rawkeys = NULL;
    rio->scan.rawvals = NULL;
    rio->err = NULL;
}

void RIOInitDeleteRange(RIO *rio, int cf, sds start_key, sds end_key) {
    rio->action = ROCKS_DELETERANGE;
    rio->delete_range.cf = cf;
    rio->delete_range.start_key = start_key;
    rio->delete_range.end_key = end_key;
    rio->err = NULL;
}

void RIODeinit(RIO *rio) {
    int i;

    switch (rio->action) {
    case  ROCKS_GET:
        sdsfree(rio->get.rawkey);
        rio->get.rawkey = NULL;
        sdsfree(rio->get.rawval);
        rio->get.rawval = NULL;
        break;
    case  ROCKS_PUT:
        sdsfree(rio->put.rawkey);
        rio->put.rawkey = NULL;
        sdsfree(rio->put.rawval);
        rio->put.rawval = NULL;
        break;
    case  ROCKS_DEL:
        sdsfree(rio->del.rawkey);
        rio->del.rawkey = NULL;
        break;
    case  ROCKS_MULTIGET:
        for (i = 0; i < rio->multiget.numkeys; i++) {
            if (rio->multiget.rawkeys) sdsfree(rio->multiget.rawkeys[i]);
            if (rio->multiget.rawvals) sdsfree(rio->multiget.rawvals[i]);
        }
        zfree(rio->multiget.cfs);
        rio->multiget.cfs = NULL;
        zfree(rio->multiget.rawkeys);
        rio->multiget.rawkeys = NULL;
        zfree(rio->multiget.rawvals);
        rio->multiget.rawvals = NULL;
        break;
    case  ROCKS_SCAN:
        sdsfree(rio->scan.prefix);
        for (i = 0; i < rio->scan.numkeys; i++) {
            if (rio->scan.rawkeys) sdsfree(rio->scan.rawkeys[i]);
            if (rio->scan.rawvals) sdsfree(rio->scan.rawvals[i]);
        }
        zfree(rio->scan.rawkeys);
        rio->scan.rawkeys = NULL;
        zfree(rio->scan.rawvals);
        rio->scan.rawvals = NULL;
        break;
    case  ROCKS_WRITE:
        rocksdb_writebatch_destroy(rio->write.wb);
        rio->write.wb = NULL;
        break;
    case  ROCKS_DELETERANGE:
        sdsfree(rio->delete_range.start_key);
        rio->delete_range.start_key = NULL;
        sdsfree(rio->delete_range.end_key);
        rio->delete_range.end_key = NULL;
        break;
    default:
        break;
    }
}

static inline rocksdb_column_family_handle_t *rioGetCF(int cf) {
    switch (cf) {
    case META_CF:
        return server.rocks->meta_cf;
    case DATA_CF:
        return server.rocks->default_cf;
    case SCORE_CF:
        return server.rocks->score_cf;
    default:
        return NULL;
    }
}

static int doRIOGet(RIO *rio) {
    size_t vallen;
    char *err = NULL, *val;

    val = rocksdb_get_cf(server.rocks->db, server.rocks->ropts,
            rioGetCF(rio->get.cf),
            rio->get.rawkey, sdslen(rio->get.rawkey), &vallen, &err);
    if (err != NULL) {
        rio->err = sdsnew(err);
        serverLog(LL_WARNING,"[rocks] do rocksdb get failed: %s", err);
        zlibc_free(err);
        return -1;
    }
    if (val == NULL) {
        rio->get.rawval = NULL;
    } else  {
        rio->get.rawval = sdsnewlen(val, vallen);
        zlibc_free(val);
    }

    return 0;
}

static int doRIOPut(RIO *rio) {
    char *err = NULL;
    rocksdb_put_cf(server.rocks->db, server.rocks->wopts,rioGetCF(rio->put.cf),
            rio->put.rawkey, sdslen(rio->put.rawkey),
            rio->put.rawval, sdslen(rio->put.rawval), &err);
    if (err != NULL) {
        rio->err = sdsnew(err);
        serverLog(LL_WARNING,"[rocks] do rocksdb write failed: %s", err);
        zlibc_free(err);
        return -1;
    }
    return 0;
}

static int doRIODel(RIO *rio) {
    char *err = NULL;
    rocksdb_delete_cf(server.rocks->db, server.rocks->wopts,
            rioGetCF(rio->del.cf),
            rio->del.rawkey, sdslen(rio->del.rawkey), &err);
    if (err != NULL) {
        rio->err = sdsnew(err);
        serverLog(LL_WARNING,"[rocks] do rocksdb del failed: %s", err);
        zlibc_free(err);
        return -1;
    }
    return 0;
}

static int doRIOWrite(RIO *rio) {
    char *err = NULL;
    rocksdb_write(server.rocks->db, server.rocks->wopts,
            rio->write.wb, &err);
    if (err != NULL) {
        rio->err = sdsnew(err);
        serverLog(LL_WARNING,"[rocks] do rocksdb write failed: %s", err);
        zlibc_free(err);
        return -1;
    }
    return 0;
}

static int doRIOMultiGet(RIO *rio) {
    int ret = 0, i;
    rocksdb_column_family_handle_t **cfs_list;
    char **keys_list = zmalloc(rio->multiget.numkeys*sizeof(char*));
    char **values_list = zmalloc(rio->multiget.numkeys*sizeof(char*));
    size_t *keys_list_sizes = zmalloc(rio->multiget.numkeys*sizeof(size_t));
    size_t *values_list_sizes = zmalloc(rio->multiget.numkeys*sizeof(size_t));
    char **errs = zmalloc(rio->multiget.numkeys*sizeof(char*));
    cfs_list = zmalloc(rio->multiget.numkeys*sizeof(rocksdb_column_family_handle_t*));

    for (i = 0; i < rio->multiget.numkeys; i++) {
        cfs_list[i] = rioGetCF(rio->multiget.cfs[i]);
        keys_list[i] = rio->multiget.rawkeys[i];
        keys_list_sizes[i] = sdslen(rio->multiget.rawkeys[i]);
    }

    rocksdb_multi_get_cf(server.rocks->db, server.rocks->ropts,
            (const rocksdb_column_family_handle_t *const *)cfs_list,
            rio->multiget.numkeys,
            (const char**)keys_list, (const size_t*)keys_list_sizes,
            values_list, values_list_sizes, errs);

    rio->multiget.rawvals = zmalloc(rio->multiget.numkeys*sizeof(sds));
    for (i = 0; i < rio->multiget.numkeys; i++) {
        if (values_list[i] == NULL || values_list_sizes[i] == 0) {
            rio->multiget.rawvals[i] = NULL;
        } else {
            rio->multiget.rawvals[i] = sdsnewlen(values_list[i],
                    values_list_sizes[i]);
            zlibc_free(values_list[i]);
        }
        if (errs[i]) {
            if (rio->err == NULL) {
                rio->err = sdsnew(errs[i]);
                serverLog(LL_WARNING,"[rocks] do rocksdb multiget failed: %s",
                        rio->err);
            }
            zlibc_free(errs[i]);
        }
    }

    if (rio->err != NULL) {
        ret = -1;
        goto end;
    }

end:
    zfree(cfs_list);
    zfree(keys_list);
    zfree(values_list);
    zfree(keys_list_sizes);
    zfree(values_list_sizes);
    zfree(errs);
    return ret;
}

static int doRIOScan(RIO *rio) {
    int ret = 0;
    char *err = NULL;
    rocksdb_iterator_t *iter = NULL;
    sds prefix = rio->scan.prefix;
    size_t numkeys = 0, numalloc = 8;
    sds *rawkeys = zmalloc(numalloc*sizeof(sds));
    sds *rawvals = zmalloc(numalloc*sizeof(sds));

    iter = rocksdb_create_iterator_cf(server.rocks->db,server.rocks->ropts,
            rioGetCF(rio->scan.cf));
    rocksdb_iter_seek(iter,prefix,sdslen(prefix));

    while (rocksdb_iter_valid(iter)) {
        size_t klen, vlen;
        const char *rawkey, *rawval;
        rawkey = rocksdb_iter_key(iter, &klen);

        if (klen < sdslen(prefix) || memcmp(rawkey, prefix, sdslen(prefix)))
            break;

        numkeys++;
        /* make room for key/val */
        if (numkeys >= numalloc) {
            if (numalloc >= RIO_SCAN_NUMKEYS_ALLOC_LINER) {
                numalloc += RIO_SCAN_NUMKEYS_ALLOC_LINER;
            } else {
                numalloc *= 2;
            }
            rawkeys = zrealloc(rawkeys, numalloc*sizeof(sds));
            rawvals = zrealloc(rawvals, numalloc*sizeof(sds));
        }

        rawval = rocksdb_iter_value(iter, &vlen);
        rawkeys[numkeys-1] = sdsnewlen(rawkey, klen);
        rawvals[numkeys-1] = sdsnewlen(rawval, vlen);

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_get_error(iter, &err);
    if (err != NULL) {
        rio->err = err;
        serverLog(LL_WARNING,"[rocks] do rocksdb scan failed: %s", err);
        ret = -1;
    }
    
    rio->scan.numkeys = numkeys;
    rio->scan.rawkeys = rawkeys;
    rio->scan.rawvals = rawvals;
    rocksdb_iter_destroy(iter);

    return ret;
}

static int doRIODeleteRange(RIO *rio) {
    char *err = NULL;
    rocksdb_delete_range_cf(server.rocks->db, server.rocks->wopts,
            server.rocks->default_cf,
            rio->delete_range.start_key, sdslen(rio->delete_range.start_key),
            rio->delete_range.end_key, sdslen(rio->delete_range.end_key), &err);
    if (err != NULL) {
        rio->err = sdsnew(err);
        serverLog(LL_WARNING,"[rocks] do rocksdb delete range failed: %s", err);
        zlibc_free(err);
        return -1;
    }
    return 0;
}

void dumpRIO(RIO *rio) {
    sds repr = sdsnew("[ROCKS] ");
    switch (rio->action) {
    case ROCKS_GET:
        repr = sdscat(repr, "GET rawkey=");
        repr = sdscatrepr(repr, rio->get.rawkey, sdslen(rio->get.rawkey));
        repr = sdscat(repr, ", rawval=");
        if (rio->get.rawval) {
            repr = sdscatrepr(repr, rio->get.rawval, sdslen(rio->get.rawval));
        } else {
            repr = sdscatfmt(repr, "<nil>");
        }
        break;
    case ROCKS_PUT:
        repr = sdscat(repr, "PUT rawkey=");
        repr = sdscatrepr(repr, rio->put.rawkey, sdslen(rio->put.rawkey));
        repr = sdscat(repr, ", rawval=");
        repr = sdscatrepr(repr, rio->put.rawval, sdslen(rio->put.rawval));
        break;
    case ROCKS_DEL:
        repr = sdscat(repr, "DEL ");
        repr = sdscatrepr(repr, rio->del.rawkey, sdslen(rio->del.rawkey));
        break;
    case ROCKS_WRITE:
        repr = sdscat(repr, "WRITE ");
        break;
    case ROCKS_MULTIGET:
        repr = sdscat(repr, "MULTIGET:\n");
        for (int i = 0; i < rio->multiget.numkeys; i++) {
            repr = sdscat(repr, "  (");
            repr = sdscatrepr(repr, rio->multiget.rawkeys[i],
                    sdslen(rio->multiget.rawkeys[i]));
            repr = sdscat(repr, ")=>(");
            if (rio->multiget.rawvals[i]) {
                repr = sdscatrepr(repr, rio->multiget.rawvals[i],
                        sdslen(rio->multiget.rawvals[i]));
            } else {
                repr = sdscatfmt(repr, "<nil>");
            }
            repr = sdscat(repr,")\n");
        }
        break;
    case ROCKS_SCAN:
        repr = sdscat(repr, "SCAN:(");
        repr = sdscatrepr(repr, rio->scan.prefix, sdslen(rio->scan.prefix));
        repr = sdscat(repr, ")\n");
        for (int i = 0; i < rio->scan.numkeys; i++) {
            repr = sdscat(repr, "  (");
            repr = sdscatrepr(repr, rio->scan.rawkeys[i],
                    sdslen(rio->scan.rawkeys[i]));
            repr = sdscat(repr, ")=>(");
            repr = sdscatrepr(repr, rio->scan.rawvals[i],
                    sdslen(rio->scan.rawvals[i]));
            repr = sdscat(repr,")\n");
        }
        break;
    case ROCKS_DELETERANGE:
        repr = sdscat(repr, "DELETERANGE start_key=%s");
        repr = sdscatrepr(repr, rio->delete_range.start_key, sdslen(rio->delete_range.start_key));
        repr = sdscat(repr, ", end_key=");
        repr = sdscatrepr(repr, rio->delete_range.end_key, sdslen(rio->delete_range.end_key));
        break;
    default:
        serverPanic("[rocks] Unknown io action: %d", rio->action);
        break;
    }
    serverLog(LL_NOTICE, "%s", repr);
    sdsfree(repr);
}

int doRIO(RIO *rio) {
    int ret;
    if (server.debug_rio_latency) usleep(server.debug_rio_latency*1000);

    switch (rio->action) {
    case ROCKS_GET:
        ret = doRIOGet(rio);
        break;
    case ROCKS_PUT:
        ret = doRIOPut(rio);
        break;
    case ROCKS_DEL:
        ret = doRIODel(rio);
        break;
    case ROCKS_WRITE:
        ret = doRIOWrite(rio);
        break;
    case ROCKS_MULTIGET:
        ret = doRIOMultiGet(rio);
        break;
    case ROCKS_SCAN:
        ret = doRIOScan(rio);
        break;
    case ROCKS_DELETERANGE:
        ret = doRIODeleteRange(rio);
        break;
    default:
        serverPanic("[rocks] Unknown io action: %d", rio->action);
        return -1;
    }

#ifdef ROCKS_DEBUG
    dumpRIO(rio);
#endif

    return ret;
}

static void doNotify(swapRequest *req) {
    req->notify_cb(req, req->notify_pd);
}

static inline int doSwapDelMeta(swapData *data) {
    int retval;
    RIO rio_, *rio = &rio_;
    sds rawkey = swapDataEncodeMetaKey(data);
    RIOInitDel(rio,META_CF,rawkey);
    retval = doRIO(rio);
    RIODeinit(rio);
    return retval;
}

static int executeSwapDelRequest(swapRequest *req) {
    int i, numkeys, retval = EXEC_OK, action;
    int *cfs = NULL;
    sds *rawkeys = NULL;
    RIO _rio = {0}, *rio = &_rio;
    rocksdb_writebatch_t *wb;
    swapData *data = req->data;

    retval = doSwapDelMeta(data);
    DEBUG_MSGS_APPEND(req->msgs,"execswap-del.meta","retval=%d",retval);
    if (retval) goto end;

    if (swapDataEncodeKeys(data,req->intention,req->datactx,
                &action,&numkeys,&cfs,&rawkeys)) {
        retval = EXEC_FAIL;
        goto end;
    }
    DEBUG_MSGS_APPEND(req->msgs,"execswap-del-encodekeys",
            "action=%s, numkeys=%d", rocksActionName(action), numkeys);

    if (numkeys == 0) goto end;

    if (action == ROCKS_WRITE) {
        wb = rocksdb_writebatch_create();
        for (i = 0; i < numkeys; i++) {
            rocksdb_writebatch_delete_cf(wb, rioGetCF(cfs[i]),
                    rawkeys[i], sdslen(rawkeys[i]));
        }
        DEBUG_MSGS_APPEND(req->msgs,"execswap-write","numkeys=%d.",numkeys);
        RIOInitWrite(rio,wb);
        //TODO confirm: rawkeys leak?
    } else if (action == ROCKS_DEL) {
        serverAssert(numkeys == 1 && rawkeys);
        DEBUG_MSGS_APPEND(req->msgs,"execswap-del-del","rawkey=%s",rawkeys[0]);
        RIOInitDel(rio,cfs[0],rawkeys[0]);
        zfree(rawkeys), rawkeys = NULL;
        zfree(cfs), cfs = NULL;
    } else if (action == ROCKS_DELETERANGE) {
        serverAssert(numkeys == 2 && rawkeys);
        DEBUG_MSGS_APPEND(req->msgs,"execswap-del-deleterange",
                "start_key=%s end_key=%s",rawkeys[0],rawkeys[1]);
        RIOInitDeleteRange(rio,cfs[0],rawkeys[0],rawkeys[1]);
        zfree(rawkeys), rawkeys = NULL;
        zfree(cfs), cfs = NULL;
    } else {
        retval = EXEC_FAIL;
        goto end;
    }

    updateStatsSwapRIO(req, rio);
    if (doRIO(rio)) {
        retval = EXEC_FAIL;
        goto end;
    }

end:
    doNotify(req);
    RIODeinit(rio);
    return retval;
}

static inline int doSwapOutMeta(swapData *data) {
    int retval;
    RIO rio_, *rio = &rio_;
    sds rawkey = swapDataEncodeMetaKey(data);
    sds rawval = swapDataEncodeMetaVal(data);
    RIOInitPut(rio,META_CF,rawkey,rawval);
    retval = doRIO(rio);
    RIODeinit(rio);
    return retval;
}

static int executeSwapOutRequest(swapRequest *req) {
    int i, numkeys, retval = EXEC_OK, action, *cfs = NULL;
    sds *rawkeys = NULL, *rawvals = NULL;
    RIO _rio = {0}, *rio = &_rio;
    rocksdb_writebatch_t *wb = NULL;
    swapData *data = req->data;

    if (req->intention_flags & SWAP_EXEC_OUT_META) {
        retval = doSwapOutMeta(data);
        DEBUG_MSGS_APPEND(req->msgs,"execswap-out.meta","retval=%d",retval);
        if (retval != EXEC_OK) goto end;
    }

    if (swapDataEncodeData(data,req->intention,req->datactx,
                &action,&numkeys,&cfs,&rawkeys,&rawvals)) {
        retval = EXEC_FAIL;
        goto end;
    }
    DEBUG_MSGS_APPEND(req->msgs,"execswap-out-encodedata",
            "action=%s, numkeys=%d", rocksActionName(action), numkeys);

    if (numkeys <= 0) goto end;

    if (action == ROCKS_PUT) {
        serverAssert(numkeys == 1);

#ifdef SWAP_DEBUG
        sds rawval_repr = sdscatrepr(sdsempty(), rawvals[0], sdslen(rawvals[0]));
        DEBUG_MSGS_APPEND(req->msgs,"execswap-out-put","rawkey=%s,rawval=%s",
                rawkeys[0], rawval_repr);
        sdsfree(rawval_repr);
#endif

        RIOInitPut(rio,cfs[0],rawkeys[0],rawvals[0]);
        zfree(cfs), cfs = NULL;
        zfree(rawkeys), rawkeys = NULL;
        zfree(rawvals), rawvals = NULL;
    } else if (action == ROCKS_WRITE) {
        wb = rocksdb_writebatch_create();
        for (i = 0; i < numkeys; i++) {
            rocksdb_writebatch_put_cf(wb,rioGetCF(cfs[i]),
                    rawkeys[i],sdslen(rawkeys[i]),
                    rawvals[i], sdslen(rawvals[i]));
        }
        DEBUG_MSGS_APPEND(req->msgs,"execswap-out-write","numkeys=%d",numkeys);
        RIOInitWrite(rio, wb);
        // TODO confirm rawkeys leaked?
    } else {
        retval = EXEC_FAIL;
        goto end;
    }

    updateStatsSwapRIO(req,rio);
    if (doRIO(rio)) {
        retval = EXEC_FAIL;
        goto end;
    }

    if (swapDataCleanObject(data,req->datactx)) {
        retval = EXEC_FAIL;
        goto end;
    }
    DEBUG_MSGS_APPEND(req->msgs,"execswap-out-cleanobject","ok");

end:

    DEBUG_MSGS_APPEND(req->msgs,"execswap-out-end","retval=%d",retval);
    doNotify(req);
    if (cfs) zfree(cfs);
    if (rawkeys) {
        for (i = 0; i < numkeys; i++) {
            sdsfree(rawkeys[i]);
        }
        zfree(rawkeys);
    }
    if (rawvals) {
        for (i = 0; i < numkeys; i++) {
            sdsfree(rawvals[i]);
        }
        zfree(rawvals);
    }
    RIODeinit(rio);
    return retval;
}

static int doSwapIntentionDel(swapRequest *req, int numkeys, int *cfs, sds *rawkeys) {
    RIO _rio = {0}, *rio = &_rio;
    int i, retval;
    UNUSED(req);

    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
    for (i = 0; i < numkeys; i++) {
        rocksdb_writebatch_delete_cf(wb,rioGetCF(cfs[i]),rawkeys[i],sdslen(rawkeys[i]));
    }

    RIOInitWrite(rio, wb);
    updateStatsSwapRIO(req,rio);
    retval = doRIO(rio);
    RIODeinit(rio);

    DEBUG_MSGS_APPEND(req->msgs,"execswap-in.del","numkeys=%d,retval=%d",
            numkeys, retval);
    RIODeinit(rio);
    return retval;
}

static int executeSwapInRequest(swapRequest *req) {
    robj *decoded;
    int numkeys, retval = EXEC_OK, action, *cfs = NULL;
    sds *rawkeys = NULL;
    RIO _rio = {0}, *rio = &_rio;
    swapData *data = req->data;

    if (swapDataEncodeKeys(data,req->intention,req->datactx,
                &action,&numkeys,&cfs,&rawkeys)) {
        retval = EXEC_FAIL;
        goto end;
    }
    DEBUG_MSGS_APPEND(req->msgs,"execswap-in-encodekeys","action=%s, numkeys=%d",
            rocksActionName(action),numkeys);

    if (numkeys <= 0) goto end;

    if (action == ROCKS_MULTIGET) {
        RIOInitMultiGet(rio,numkeys,cfs,rawkeys);
        if (doRIO(rio)) {
            retval = EXEC_FAIL;
            goto end;
        }
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-multiget",
                "numkeys=%d,rio=ok", numkeys);

        if (swapDataDecodeData(data,rio->multiget.numkeys,rio->multiget.cfs,
                    rio->multiget.rawkeys,rio->multiget.rawvals,&decoded)) {
            retval = EXEC_FAIL;
            goto end;
        }
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-decodedata","decoded=%p",(void*)decoded);

        if (req->intention_flags & SWAP_EXEC_IN_DEL) {
            if (doSwapIntentionDel(req,numkeys,cfs,rawkeys)) {
                retval = EXEC_FAIL;
                goto end;
            }

            if (doSwapDelMeta(data)) {
                retval = EXEC_FAIL;
                goto end;
            }
        }
    } else if (action == ROCKS_GET) {
        serverAssert(numkeys == 1);
        RIOInitGet(rio, cfs[0], rawkeys[0]);
        if (doRIO(rio)) {
            retval = EXEC_FAIL;
            goto end;
        }

#ifdef SWAP_DEBUG
        sds rawval_repr = sdscatrepr(sdsempty(),rio->get.rawval,
                sdslen(rio->get.rawval));
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-get","rawkey=%s,rawval=%s",rawkeys[0],rawval_repr);
        sdsfree(rawval_repr);
#endif

        if (swapDataDecodeData(data,1,&rio->get.cf,&rio->get.rawkey,
                    &rio->get.rawval,&decoded)) {
            retval = EXEC_FAIL;
            goto end;
        }
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-decodedata","decoded=%p",(void*)decoded);

        if (req->intention_flags & SWAP_EXEC_IN_DEL) {
            if (doSwapIntentionDel(req,numkeys,cfs,rawkeys)) {
                retval = EXEC_FAIL;
                goto end;
            }

            if (doSwapDelMeta(data)) {
                retval = EXEC_FAIL;
                goto end;
            }
        }
        /* rawkeys not moved, only rakeys[0] moved, free when done. */
        zfree(cfs);
        zfree(rawkeys);
    } else if (action == ROCKS_SCAN) {
        int *tmpcfs, i;
        serverAssert(numkeys == 1);
        RIOInitScan(rio,cfs[0],rawkeys[0]);
        if (doRIO(rio)) {
            retval = EXEC_FAIL;
            goto end;
        }
        tmpcfs = zmalloc(sizeof(int)*rio->scan.numkeys);
        for (i = 0; i < rio->scan.numkeys; i++) tmpcfs[i] = cfs[0];
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-scan","prefix=%s,rio=ok",rawkeys[0]);
        if (swapDataDecodeData(data,rio->scan.numkeys,tmpcfs,rio->scan.rawkeys,
                    rio->scan.rawvals,&decoded)) {
            retval = EXEC_FAIL;
            zfree(tmpcfs);
            goto end;
        }
        zfree(tmpcfs);
        DEBUG_MSGS_APPEND(req->msgs,"execswap-in-decodedata", "decoded=%p",(void*)decoded);

        if (req->intention_flags & SWAP_EXEC_IN_DEL) {
            if (doSwapIntentionDel(req,numkeys,cfs,rawkeys)) {
                retval = EXEC_FAIL;
                goto end;
            }

            if (doSwapDelMeta(data)) {
                retval = EXEC_FAIL;
                goto end;
            }
        }
        /* rawkeys not moved, only rakeys[0] moved, free when done. */
        zfree(cfs);
        zfree(rawkeys);
    } else {
        retval = EXEC_FAIL;
        goto end;
    }

    req->result = swapDataCreateOrMergeObject(data,decoded,req->datactx);
    DEBUG_MSGS_APPEND(req->msgs,"execswap-in-createormerge","result=%p",(void*)req->result);

end:
    updateStatsSwapRIO(req,rio);
    DEBUG_MSGS_APPEND(req->msgs,"execswap-in-end","retval=%d",retval);
    doNotify(req);
    RIODeinit(rio);
    return retval;
}

static inline int RIOGetNotFound(RIO *rio) {
    serverAssert(rio->action == ROCKS_GET);
    return rio->get.rawval == NULL && rio->err == NULL;
}

static int swapRequestSwapInMeta(swapRequest *req) {
    int retval = EXEC_OK;
    RIO _rio = {0}, *rio = &_rio;
    sds rawkey;
    swapData *data = req->data;

    rawkey = swapDataEncodeMetaKey(data);
    if (rawkey == NULL) {
        retval = EXEC_FAIL;
        goto end;
    }

    RIOInitGet(rio,META_CF,rawkey);
    if (doRIO(rio)) {
        retval = EXEC_FAIL;
        goto end;
    }

    if (RIOGetNotFound(rio)) {
        retval = EXEC_OK;
        goto end;
    }

    if (swapDataDecodeAndSetupMeta(data,rio->get.rawval,&req->datactx)) {
        retval = EXEC_FAIL;
        goto end;
    }

    /* swap in meta into keyspace for cold keys */
    if (req->intention == SWAP_IN) {
        data->swapin_meta = 1;
    }

end:
    if (rawkey) sdsfree(rawkey);
    return retval;
}

static inline int swapRequestIsMetaType(swapRequest *req) {
    return req->key_request != NULL;
}

static int executeSwapRequest(swapRequest *req) {
    /* do execute swap */ 
    switch (req->intention) {
    case SWAP_IN: return executeSwapInRequest(req);
    case SWAP_OUT: return executeSwapOutRequest(req);
    case SWAP_DEL: return executeSwapDelRequest(req);
    case SWAP_NOP: return EXEC_OK;
    default: return EXEC_FAIL;
    }
}

static inline void swapRequestSetIntention(swapRequest *req, int intention,
        uint32_t intention_flags) {
    req->intention = intention;
    req->intention_flags = intention_flags;
}

int processSwapRequest(swapRequest *req) {
    int retval = EXEC_OK, intention;
    swapData *data = req->data;
    uint32_t intention_flags;
    void *datactx = req->datactx;

    if (!swapRequestIsMetaType(req))
        return executeSwapRequest(req);

    if ((retval = swapRequestSwapInMeta(req)))
        return retval;

    /* key confirmed not exists, no need to execute swap request. */
    if (!swapDataAlreadySetup(req->data)) {
        doNotify(req);
        return EXEC_OK;
    }

    if ((retval = swapDataAna(data,req->key_request,
                    &intention,&intention_flags,datactx))) {
        return retval;
    }

    swapRequestSetIntention(req,intention,intention_flags);

    return executeSwapRequest(req);
}

/* Called by async-complete-queue or parallel-sync in server thread
 * to swap in/out/del data */
int finishSwapRequest(swapRequest *req) {
    DEBUG_MSGS_APPEND(req->msgs,"execswap-finish","intention=%s",
            swapIntentionName(req->intention));
    int ret;

    switch(req->intention) {
    case SWAP_IN:
        ret = swapDataSwapIn(req->data,req->result,req->datactx);
        if (ret == 0) ret = swapDataSwapInMeta(req->data);
        return ret;
    case SWAP_OUT:
        return swapDataSwapOut(req->data,req->datactx);
    case SWAP_DEL: 
        return swapDataSwapDel(req->data,req->datactx,
                req->intention_flags & SWAP_FIN_DEL_SKIP);
    default:
        return -1;
    }
}

static inline void submitSwapRequest(int mode,swapRequest *req) {
    updateStatsSwapStart(req);
    if (mode == SWAP_MODE_ASYNC) {
        asyncSwapRequestSubmit(req);
    } else {
        parallelSyncSwapRequestSubmit(req);
    }
}

void submitSwapMetaRequest(int mode,keyRequest *key_request,swapData* data,
        void *datactx, swapRequestFinishedCallback cb, void *pd, void *msgs) {
    swapRequest *req = swapRequestNew(key_request,-1,-1,data,datactx,cb,pd,msgs);
    submitSwapRequest(mode,req);
}

void submitSwapDataRequest(int mode, int intention,
        uint32_t intention_flags, swapData* data, void *datactx,
        swapRequestFinishedCallback cb, void *pd, void *msgs) {
    swapRequest *req = swapRequestNew(NULL,intention,intention_flags,data,datactx,cb,pd,msgs);
    submitSwapRequest(mode,req);
}

swapRequest *swapRequestNew(keyRequest *key_request, int intention,
        uint32_t intention_flags, swapData *data, void *datactx,
        swapRequestFinishedCallback cb, void *pd, void *msgs) {
    swapRequest *req = zcalloc(sizeof(swapRequest));
    UNUSED(msgs);
    req->key_request = key_request;
    req->intention = intention;
    req->intention_flags = intention_flags;
    req->data = data;
    req->datactx = datactx;
    req->result = NULL;
    req->finish_cb = cb;
    req->finish_pd = pd;
    req->swap_memory = 0;
#ifdef SWAP_DEBUG
    req->msgs = msgs;
#endif
    return req;
}

void swapRequestFree(swapRequest *req) {
    if (req->result) decrRefCount(req->result);
    zfree(req);
}

#ifdef REDIS_TEST

int mockNotifyCallback(swapRequest *req, void *pd) {
    UNUSED(req),UNUSED(pd);
    return 0;
}

int wholeKeyRocksDataExists(redisDb *db, robj *key) {
    size_t vlen;
    char *err = NULL, *rawval = NULL;
    sds rawkey = rocksEncodeDataKey(db,key->ptr,NULL);
    rawval = rocksdb_get_cf(server.rocks->db, server.rocks->ropts,rioGetCF(DATA_CF),rawkey,sdslen(rawkey),&vlen,&err);
    serverAssert(err == NULL);
    zlibc_free(rawval);
    return rawval != NULL;
}

int wholeKeyRocksMetaExists(redisDb *db, robj *key) {
    size_t vlen;
    char *err = NULL, *rawval = NULL;
    sds rawkey = rocksEncodeMetaKey(db,key->ptr);
    rawval = rocksdb_get_cf(server.rocks->db, server.rocks->ropts,rioGetCF(META_CF),rawkey,sdslen(rawkey),&vlen,&err);
    serverAssert(err == NULL);
    zlibc_free(rawval);
    return rawval != NULL;
}

int doRocksdbFlush() {
    char *err = NULL;
    rocksdb_flushoptions_t *flushopts = rocksdb_flushoptions_create();
    rocksdb_flush_cf(server.rocks->db, flushopts, server.rocks->meta_cf, &err);
    rocksdb_flush_cf(server.rocks->db, flushopts, server.rocks->default_cf, &err);
    rocksdb_flush_cf(server.rocks->db, flushopts, server.rocks->score_cf, &err);
    serverAssert(err == NULL);
    rocksdb_flushoptions_destroy(flushopts);
    return 0;
}

void initServer(void);
void initServerConfig(void);
void InitServerLast();
int swapExecTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;
    server.hz = 10;
    sds rawkey1 = sdsnew("rawkey1"), rawkey2 = sdsnew("rawkey2");
    sds rawval1 = sdsnew("rawval1"), rawval2 = sdsnew("rawval2");
    sds prefix = sdsnew("rawkey");
    robj *key1 = createStringObject("key1",4);
    robj *val1 = createStringObject("val1",4);
    initTestRedisDb();
    redisDb *db = server.db;
    long long EXPIRE = 3000000000LL * 1000;

    keyRequest key1_req_, *key1_req = &key1_req_;
    key1_req->level = REQUEST_LEVEL_KEY;
    key1_req->num_subkeys = 0;
    key1_req->key = createStringObject("key1",4);
    key1_req->subkeys = NULL;

    TEST("exec: init") {
        initServerConfig();
        incrRefCount(val1);
        dbAdd(db,key1,val1);
        setExpire(NULL,db,key1,EXPIRE);
        rocksInit();
        initStatsSwap();
    }

   TEST("exec: rio") {
       rocksdb_writebatch_t *wb;
       RIO _rio, *rio = &_rio;

       RIOInitPut(rio,DATA_CF,rawkey1,rawval1);
       test_assert(doRIO(rio) == C_OK);

       RIOInitGet(rio,DATA_CF,rawkey1);
       test_assert(doRIO(rio) == C_OK);
       test_assert(sdscmp(rio->get.rawval, rawval1) == 0);


       RIOInitDel(rio,DATA_CF,rawkey1);
       test_assert(doRIO(rio) == C_OK);

       wb = rocksdb_writebatch_create();
       rocksdb_writebatch_put_cf(wb,rioGetCF(DATA_CF),rawkey1,sdslen(rawkey1),rawval1,sdslen(rawval1));
       rocksdb_writebatch_put_cf(wb,rioGetCF(DATA_CF),rawkey2,sdslen(rawkey2),rawval2,sdslen(rawval2));
       RIOInitWrite(rio,wb);
       test_assert(doRIO(rio) == C_OK);

       sds *rawkeys = zmalloc(sizeof(sds)*2);
       int *cfs = zmalloc(sizeof(int)*2);
       rawkeys[0] = rawkey1, cfs[0] = DATA_CF;
       rawkeys[1] = rawkey2, cfs[1] = DATA_CF;
       RIOInitMultiGet(rio,2,cfs,rawkeys);
       test_assert(doRIO(rio) == C_OK);
       test_assert(rio->multiget.numkeys == 2);
       test_assert(sdscmp(rio->multiget.rawvals[0],rawval1) == 0);
       test_assert(sdscmp(rio->multiget.rawvals[1],rawval2) == 0);

       RIOInitScan(rio,DATA_CF,prefix);
       test_assert(doRIO(rio) == C_OK);
       test_assert(rio->scan.numkeys == 2);
       test_assert(sdscmp(rio->scan.rawvals[0],rawval1) == 0);
       test_assert(sdscmp(rio->scan.rawvals[1],rawval2) == 0);
   }

   TEST("exec: swap-out hot string") {
       val1 = lookupKey(db,key1,LOOKUP_NOTOUCH);
       test_assert(val1 != NULL);
       test_assert(getExpire(db,key1) == EXPIRE);
       swapData *data = createWholeKeySwapDataWithExpire(db,key1,val1,EXPIRE,NULL);
       swapRequest *req = swapRequestNew(NULL/*!cold*/,SWAP_OUT,SWAP_EXEC_OUT_META,data,NULL,NULL,NULL,NULL);
       req->notify_cb = mockNotifyCallback;
       req->notify_pd = NULL;
       test_assert(processSwapRequest(req) == 0);
       test_assert(finishSwapRequest(req) == 0);
       test_assert(lookupKey(db,key1,LOOKUP_NOTOUCH) == NULL);
       test_assert(getExpire(db,key1) == -1);
       test_assert(wholeKeyRocksDataExists(db,key1));
       test_assert(wholeKeyRocksMetaExists(db,key1));
   }

   TEST("exec: swap-in cold string") {
       /* rely on data swap out to rocksdb by previous case */
       swapData *data = createSwapData(db,key1,NULL);
       key1_req->cmd_intention = SWAP_IN;
       key1_req->cmd_intention_flags = 0;
       swapRequest *req = swapRequestNew(key1_req,-1,-1,data,NULL,NULL,NULL,NULL);
       req->notify_cb = mockNotifyCallback;
       req->notify_pd = NULL;
       test_assert(processSwapRequest(req) == 0);
       test_assert(finishSwapRequest(req) == 0);
       test_assert((val1 = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL && !objectIsDirty(val1));
       test_assert(getExpire(db,key1) == EXPIRE);
       test_assert(wholeKeyRocksDataExists(db,key1));
       test_assert(wholeKeyRocksMetaExists(db,key1));
   } 

   TEST("exec: swap-del hot string") {
       /* rely on data swap out to rocksdb by previous case */
       val1 = lookupKey(db,key1,LOOKUP_NOTOUCH);
       swapData *data = createWholeKeySwapData(db,key1,val1,NULL);
       swapRequest *req = swapRequestNew(NULL/*!cold*/,SWAP_DEL,0,data,NULL,NULL,NULL,NULL);
       req->notify_cb = mockNotifyCallback;
       req->notify_pd = NULL;
       test_assert(executeSwapRequest(req) == 0);
       test_assert(finishSwapRequest(req) == 0);
       test_assert(lookupKey(db,key1,LOOKUP_NOTOUCH) == NULL);
       test_assert(getExpire(db,key1) == -1);
       test_assert(!wholeKeyRocksDataExists(db,key1));
       test_assert(!wholeKeyRocksMetaExists(db,key1));
   }

   TEST("exec: swap-in.del") {
       incrRefCount(val1);
       dbAdd(db,key1,val1);

       /* swap out hot key1 */
       swapData *out_data = createWholeKeySwapData(db,key1,val1,NULL);
       swapRequest *out_req = swapRequestNew(NULL/*!cold*/,SWAP_OUT,SWAP_EXEC_OUT_META,out_data,NULL,NULL,NULL,NULL);
       out_req->notify_cb = mockNotifyCallback;
       out_req->notify_pd = NULL;
       test_assert(processSwapRequest(out_req) == 0);
       test_assert(finishSwapRequest(out_req) == 0);
       test_assert(lookupKey(db,key1,LOOKUP_NOTOUCH) == NULL);
       test_assert(wholeKeyRocksMetaExists(db,key1));
       test_assert(wholeKeyRocksDataExists(db,key1));

       /* In.del cold key1 */
       swapData *in_del_data = createSwapData(db,key1,NULL);
       key1_req->cmd_intention = SWAP_IN;
       key1_req->cmd_intention_flags = SWAP_IN_DEL;
       swapRequest *in_del_req = swapRequestNew(key1_req,-1,-1,in_del_data,NULL,NULL,NULL,NULL);
       in_del_req->notify_cb = mockNotifyCallback;
       in_del_req->notify_pd = NULL;
       test_assert(processSwapRequest(in_del_req) == 0);
       test_assert(finishSwapRequest(in_del_req) == 0);
       test_assert(lookupKey(db,key1,LOOKUP_NOTOUCH) != NULL);
       test_assert(!wholeKeyRocksMetaExists(db,key1));
       test_assert(!wholeKeyRocksDataExists(db,key1));
   }

   return error;
}

#endif
