/***************************************************************************
 * 
 * Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/



/**
 * @file package_format.cpp
 * @author chendazhuang(com@baidu.com)
 * @date 2013/11/20 14:18:33
 * @brief 
 *  
 **/

#include "package_format.h"
 //  #include "lcs.h"

#include <errno.h>
#include <string.h>
#include <string>
#include <map>
using std::string;
using std::map;

using namespace google::protobuf::compiler;
using namespace google::protobuf;

#include <stdio.h>

 // #include "util/log.h"
/// using namespace store;

#define log_debug(...) 
#define log_notice(...) {fprintf(stderr,__VA_ARGS__);fprintf(stderr,"\n");}
#define log_warning(...) {fprintf(stderr,__VA_ARGS__);fprintf(stderr,"\n");}

/// #include "jemalloc/jemalloc.h"
#include "msgpack.h"

#define ERR_BUF_LEN 128
#define STR_BUF_LEN 127
#define MAX_CMD_CONF_LINE_LEN 1024
#define MAX_FILE_PATH_LEN 256

#define UPS_SUFFIX "ReqIdl"
#define DOWNS_SUFFIX "ResIdl"

#define CMD_SUFFIX ".cmd"
#define PROTO_SUFFIX ".proto"

class CompilerErrorCollector : public MultiFileErrorCollector {
    public:
        void AddError(const string &filename, int line, int column, const string &message) {
            if (line == -1) {
                log_warning("compile proto file error: %s, %s", filename.c_str(), message.c_str());
            } else {
                log_warning("compile proto file error: %s:%d, %s", filename.c_str(), line+1, message.c_str());
            }
        }
};

static int map_cmd_conf_file(map<uint32_t, cmd_msg_t*> *cmd_conf_map, char *cmd_conf_file);
static int compile_cmd_proto_file(global_pkg_ctx_t *g_ctx, char *cmd_proto_file);

static void cmd_msg_free(cmd_msg_t *cmsg);
static int msgpack2pb(Message *msg, const unsigned char *msgpack, int msgpack_len);
static int parse_msgpack_object_repeated(Message *msg, const Reflection *msg_refl, const FieldDescriptor *msg_fd, msgpack_object o);
static int parse_msgpack_object(Message *msg, msgpack_object o);
static int pb2msgpack(Message *msg, unsigned char *msgpack_buf, int *msgpack_buf_len);
static int parse_msg_repeated(const Message *msg, msgpack_packer *pk, const Reflection * refl,const FieldDescriptor *field);
static int parse_msg(const Message *msg, msgpack_packer *pk);
static global_pkg_ctx_t *global_pkg_ctx_new();
static int add_service_proto(pb_ctx_t *pb_ctx, char *service_name, char *service_dir);
//static int global_pkg_ctx_free(global_pkg_ctx_t *g_ctx);
int global_pkg_ctx_free();
static void check_reload_done_nonblock();
static void* check_reload_done_block(void *arg);
static void reload_error(char *err_msg);

global_pkg_ctx_t *g_pkg_ctx = NULL;


// static pthread_once_t g_ctx_control = PTHREAD_ONCE_INIT;

/*
 *static void g_ctx_create_once() {
 *    g_pkg_ctx = (global_pkg_ctx_t*)calloc(1, sizeof(global_pkg_ctx_t));
 *    if (g_pkg_ctx == NULL) {
 *        log_warning("fail to calloc, oom?");
 *        return;
 *    }
 *}
 */

int init_global_pkg_ctx(char *proto_conf_file) {
    // only called in program start
    int ret = 0;

    g_pkg_ctx = global_pkg_ctx_new();
    if (g_pkg_ctx == NULL) {
        log_warning("fail to global_pkg_ctx_new");
        ret = -1;
        goto out;
    }

    if (proto_conf_file == NULL) {
        log_warning("proto_conf_file cannot be null");
        ret = -1;
        goto out;
    }

    g_pkg_ctx->cur = pb_ctx_new();
    if (g_pkg_ctx->cur == NULL) {
        log_warning("pb_ctx_new fail, oom?");
        ret = -1;
        goto out;
    }

    ret = pb_ctx_init(g_pkg_ctx->cur, proto_conf_file);
    if (ret != 0) {
        log_warning("fail to init pb_ctx");
        ret = -1;
        goto out;
    }

    ret = 0;
    log_notice("success to init global_pkg_ctx");

out:
    if (ret != 0) {
        global_pkg_ctx_free();
    }
    return ret;
}

pb_ctx_t *pb_ctx_new() {
    return (pb_ctx_t*)calloc(1, sizeof(pb_ctx_t));
}

int pb_ctx_init(pb_ctx_t *pb_ctx, char *proto_conf_file) {
    int ret = 0;
    char *start = NULL;
    char *end = NULL;
    char *new_start = NULL;
    int cur_line = 0;
    char err_buf[ERR_BUF_LEN] = {0};
    char line[MAX_CMD_CONF_LINE_LEN] = {0};

    FILE *fp = NULL;

    const Descriptor *msg_des = NULL;
    const Message *msg_proto = NULL;
    char op_name[MAX_CMD_CONF_LINE_LEN] = {0}; 

    int size = 0;

    pb_ctx->source_tree = new DiskSourceTree();
    pb_ctx->source_tree->MapPath("", "."); 
    pb_ctx->error_collector = new CompilerErrorCollector();
    pb_ctx->importer = new Importer(pb_ctx->source_tree, pb_ctx->error_collector);

    fp = fopen(proto_conf_file, "r");
    if (fp == NULL) {
        log_warning("fail to open proto_conf_file: %s, %s", proto_conf_file, strerror_r(errno, err_buf, ERR_BUF_LEN));
        return -1;
    }

    while (fgets(line, MAX_CMD_CONF_LINE_LEN, fp)) {
        cur_line ++;
        start = line;
        for (; *start == ' '; start ++);
        if (*start == '#' || *start == '\n' || *start == '\r') {
            continue;
        }
        end = strchr(start, ':');
        if (end == NULL) {
            log_warning("invalid proto.conf:%d, %s, ':' required", cur_line, line);
            ret = -1;
            goto out;
        }
        new_start = end + 1;

        *end = '\0';
        for (end --; *end == ' ' && end != start; *end = '\0', end --);
        if (end == start) {
            log_warning("invalid proto.conf:%d, %s, server_name required before ':'", cur_line, line);
            ret = -1;
            goto out;
        }

        for (; *new_start == ' '; new_start ++);
        end = strchr(new_start, '\n');
        if (end == NULL) {
            log_warning("invalid cmd conf, too long, %s:%d, %s", proto_conf_file, cur_line, line);
            ret = -1;
            goto out;
        }
        *end = '\0';

        for (end --; end != new_start && (*end == ' ' || *end == '\r'); *end = '\0', end --);
        end ++;
        if (end == new_start) {
            log_warning("service_dir cannot be null, %s:%d, %s", proto_conf_file, cur_line, line);
            ret = -1;
            goto out;
        }

        end = strchr(new_start, '#');
        if (end) {
            *end = '\0';
            for (end --; end != new_start && (*end == ' ' || *end == '\r'); *end = '\0', end --);
            end ++;
            if (end == new_start) {
                log_warning("service_dir cannot be null, %s:%d, %s", proto_conf_file, cur_line, line);
                ret = -1;
                goto out;
            }
        }

        ret = add_service_proto(pb_ctx, start, new_start);
        if (ret != 0) {
            log_warning("fail to add proto file for service: %s", start);
            ret = -1;
            goto out;
        }
    }

    pb_ctx->des_pool = pb_ctx->importer->pool();
    pb_ctx->msg_factory = new DynamicMessageFactory(pb_ctx->des_pool);

    map<uint32_t, cmd_msg_t*> *cmd_conf_map = pb_ctx->cmd_conf_map;
    for (map<uint32_t, cmd_msg_t*>::iterator it = cmd_conf_map->begin(); it != cmd_conf_map->end(); ++ it) {
        size = snprintf(op_name, MAX_CMD_CONF_LINE_LEN, "%s.%s%s", it->second->cmd_name, it->second->cmd_base_name, UPS_SUFFIX);
        if (size < 0 || size >= MAX_CMD_CONF_LINE_LEN) {
            log_warning("fail to snprintf for op: %s%s", it->second->cmd_name, UPS_SUFFIX);
            ret = -1;
            goto out;
        }

        msg_des = pb_ctx->des_pool->FindMessageTypeByName(op_name);
        if (msg_des == NULL) {
            log_warning("fail to find message type[%s] in proto file", op_name);
            ret = -1;
            goto out;
        }

        msg_proto = pb_ctx->msg_factory->GetPrototype(msg_des);
        it->second->ups_msg = msg_proto->New();
        if (it->second->ups_msg == NULL) {
            log_warning("fail to create message for type[%s]", op_name); 
            ret = -1;
            goto out;
        }

        size = snprintf(op_name, MAX_CMD_CONF_LINE_LEN, "%s.%s%s", it->second->cmd_name, it->second->cmd_base_name, DOWNS_SUFFIX);
        if (size < 0 || size >= MAX_CMD_CONF_LINE_LEN) {
            log_warning("fail to snprintf for op: %s%s", it->second->cmd_name, DOWNS_SUFFIX);
            ret = -1;
            goto out;
        }

        msg_des = pb_ctx->des_pool->FindMessageTypeByName(op_name);
        if (msg_des == NULL) {
            log_warning("fail to find message type[%s] in proto file", op_name);
            ret = -1;
            goto out;
        }

        msg_proto = pb_ctx->msg_factory->GetPrototype(msg_des);
        it->second->downs_msg = msg_proto->New();
        if (it->second->downs_msg == NULL) {
            log_warning("fail to create message for type[%s]", op_name); 
            ret = -1;
            goto out;
        }
    }

    // atomic_set(&(pb_ctx->count), 0);

out:
    if (fp) {
        fclose(fp);
        fp = NULL;
    }
    if (ret != 0) {
        pb_ctx_deinit(pb_ctx);
    }
    return ret;
}

static int add_service_proto(pb_ctx_t *pb_ctx, char *service_name, char *service_dir) {
    if (!pb_ctx || !service_name) {
        log_warning("invalid params, all should be not null");
        return -1;
    }

    int ret = 0;
    char err_buf[ERR_BUF_LEN];
    char cmd_file[MAX_FILE_PATH_LEN];
    char proto_file[MAX_FILE_PATH_LEN];
    int size = 0;

    char cwd[MAX_FILE_PATH_LEN];
    bool need_chdir = false;

    const FileDescriptor *file_des = NULL;

    if (getcwd(cwd, MAX_FILE_PATH_LEN) == NULL) {
        log_warning("fail to getcwd, %s", strerror_r(errno, err_buf, ERR_BUF_LEN));
        return -1;
    }

    ret = chdir(service_dir);
    if (ret != 0) {
        log_warning("fail to chdir to service_dir:%s, serviec_name:%s, %s", service_dir, service_name, strerror_r(errno, err_buf, ERR_BUF_LEN));
        ret = -1;
        goto out;
    }
    need_chdir = true;

    size = snprintf(cmd_file, MAX_FILE_PATH_LEN, "%s%s", service_name, CMD_SUFFIX);
    if (size < 0 || size >= MAX_FILE_PATH_LEN) {
        log_warning("fail to snprintf");
        ret = -1;
        goto out;
    }

    size = snprintf(proto_file, MAX_FILE_PATH_LEN, "%s%s", service_name, PROTO_SUFFIX);
    if (size < 0 || size >= MAX_FILE_PATH_LEN) {
        log_warning("fail to snprintf");
        ret = -1;
        goto out;
    }

    file_des = pb_ctx->importer->Import(proto_file);
    if (file_des == NULL) {
        log_warning("fail to compile proto file: %s", proto_file);
        ret = -1;
        goto out;
    }

    pb_ctx->cmd_conf_map = new map<uint32_t, cmd_msg_t*>();
    ret = map_cmd_conf_file(pb_ctx->cmd_conf_map, cmd_file);
    if (ret != 0) {
        log_warning("fail to map cmd: %s", cmd_file);
        ret = -1;
        goto out;
    }

out:
    if (need_chdir) {
        if (chdir(cwd) != 0) {
            log_warning("fail to chdir to working_dir:%s, %s", cwd, strerror_r(errno, err_buf, ERR_BUF_LEN));
            // FIXME: abort?
        }
    }
    // pb_ctx, let free in caller
    return ret;
}


global_pkg_ctx_t *global_pkg_ctx_new() 
{
    g_pkg_ctx = (global_pkg_ctx_t*)calloc(1, sizeof(global_pkg_ctx_t));
    if (g_pkg_ctx == NULL) {
        log_warning("fail to calloc, oom?");
        return NULL;
    }
    /*
     *if (pthread_once(&g_ctx_control, g_ctx_create_once) != 0) {
     *    log_warning("fail to pthread_once");
     *    return NULL;
     *}
     */
    return g_pkg_ctx;
}

int global_pkg_ctx_free() 
{
    if (g_pkg_ctx) 
    {
        if (g_pkg_ctx->cur)
        {
            pb_ctx_free(g_pkg_ctx->cur);
            g_pkg_ctx->cur = NULL;
        }
        
        free(g_pkg_ctx);
        /*
         *if (g_ctx->old) {
         *    pb_ctx_free(g_ctx->old);
         *    g_ctx->old = NULL;
         *}
         */
    }

    g_pkg_ctx = NULL;
    return 0;
}

void pb_ctx_deinit(pb_ctx_t *pb_ctx) {
    if (pb_ctx->cmd_conf_map) {
        for (map<uint32_t, cmd_msg_t*>::iterator it = pb_ctx->cmd_conf_map->begin(); it != pb_ctx->cmd_conf_map->end(); ++ it) {
            if (it->second) {
                cmd_msg_free(it->second);
                it->second = NULL;
            }
        }
        delete pb_ctx->cmd_conf_map;
        pb_ctx->cmd_conf_map = NULL;
    }
    if (pb_ctx->source_tree) {
        delete pb_ctx->source_tree;
        pb_ctx->source_tree = NULL;
    }
    if (pb_ctx->error_collector) {
        delete pb_ctx->error_collector;
        pb_ctx->error_collector = NULL;
    }

    if (pb_ctx->msg_factory) {
        delete pb_ctx->msg_factory;
        pb_ctx->msg_factory = NULL;
    }

    pb_ctx->des_pool = NULL;
    if (pb_ctx->importer) {
        delete pb_ctx->importer;
        pb_ctx->importer = NULL;
    }
}

void pb_ctx_free(pb_ctx_t *pb_ctx) {
    pb_ctx_deinit(pb_ctx);
    free(pb_ctx);
}

static void cmd_msg_free(cmd_msg_t *cmsg) {
    if (cmsg) {
        if (cmsg->cmd_name) {
            free(cmsg->cmd_name);
            cmsg->cmd_name = NULL;
        }
        if (cmsg->cmd_base_name) {
            free(cmsg->cmd_base_name);
            cmsg->cmd_base_name = NULL;
        }
        if (cmsg->ups_msg) {
            delete cmsg->ups_msg;
            cmsg->ups_msg = NULL;
        }
        if (cmsg->downs_msg) {
            delete cmsg->downs_msg;
            cmsg->downs_msg = NULL;
        }
        free(cmsg);
        cmsg = NULL;
    }
}


static int map_cmd_conf_file(map<uint32_t, cmd_msg_t*> *cmd_conf_map, char *cmd_conf_file) {
    int ret = 0;
    char err_buf[ERR_BUF_LEN] = {0};
    char line_buf[MAX_CMD_CONF_LINE_LEN] = {0};
    int cur_line = 0;

    FILE *fp = fopen(cmd_conf_file, "r");
    if (fp == NULL) {
        log_warning("fail to open cmd_conf_file: %s, %s", cmd_conf_file, strerror_r(errno, err_buf, ERR_BUF_LEN));
        ret = -1;
        goto out;
    }

    while (fgets(line_buf, MAX_CMD_CONF_LINE_LEN, fp)) {
        cur_line ++;

        char *start = line_buf;

        for (; *start == ' '; ++ start);
        if (*start == '#' || *start == '\n' || *start == '\r') {
            continue;
        }

        char *end = strchr(start, ':');
        if (end == NULL) {
            log_warning("invalid cmd conf, : required, %s:%d, content: %s", cmd_conf_file, cur_line, line_buf);
            ret = -1;
            goto out;
        }
        *end = '\0';

        char *new_start = end + 1;
        for (end --; end != start && *end == ' '; *end = '\0', end --);
        end ++;
        if (end == start) {
            log_warning("cmd_no cannot be null, %s:%d, %s", cmd_conf_file, cur_line, line_buf);
            ret = -1;
            goto out;
        }

        char *tmp_end = NULL;
        uint32_t cmd_no = strtoll(start, &tmp_end, 10);
        if (*tmp_end != '\0') {
            log_warning("cmd_no must be integer, %s:%d, %s", cmd_conf_file, cur_line, line_buf);
            ret = -1;
            goto out;
        }

        start = new_start;
        for (; *start == ' '; ++ start);

        end = strchr(start, '\n');
        if (end == NULL) {
            log_warning("invalid cmd conf, too long, %s:%d, %s", cmd_conf_file, cur_line, line_buf);
            ret = -1;
            goto out;
        }
        *end = '\0';

        for (end --; end != start && (*end == ' ' || *end == '\r'); *end = '\0', end --);
        end ++;
        if (end == start) {
            log_warning("op cannot be null, %s:%d, %s", cmd_conf_file, cur_line, line_buf);
            ret = -1;
            goto out;
        }

        end = strchr(start, '#');
        if (end) {
            *end = '\0';
            for (end --; end != start && *end == ' '; *end = '\0', end --);
            end ++;
            if (end == start) {
                log_warning("op cannot be null, %s:%d, %s", cmd_conf_file, cur_line, line_buf);
                ret = -1;
                goto out;
            }
        }

        if ((*cmd_conf_map)[cmd_no] != NULL) {
            log_warning("command_no already in used: %u", cmd_no);
            ret = -1;
            goto out;
        }

        cmd_msg_t *cmsg = (cmd_msg_t*)calloc(1, sizeof(*cmsg));
        if (cmsg == NULL) {
            log_warning("fail to calloc, oom ?");
            ret = -1;
            goto out;
        }
        (*cmd_conf_map)[cmd_no] = cmsg; 

        cmsg->cmd_name = strdup(start);
        if (cmsg->cmd_name == NULL) {
            log_warning("fail to strdup, oom?");
            ret = -1;
            goto out;
        }

        end = strrchr(start, '.');
        if (end == NULL) {
            cmsg->cmd_base_name = strdup(start);
        } else {
            if (*(end+1) == '\0') {
                log_warning("invalid cmd_name, '.'cannot be last char, cmd_no: %u, cmd_name: %s", cmd_no, start);
                ret = -1;
                goto out;
            }
            cmsg->cmd_base_name = strdup(end + 1);
        }
        if (cmsg->cmd_base_name == NULL) {
            log_warning("fail to strdup, oom?");
            ret = -1;
            goto out;
        }


        log_debug("read one cmd: [%u] => [%s]", cmd_no, start);
    }

    if (!feof(fp)) {
        log_warning("fail to fgets, read cmd conf file error: %s", strerror_r(errno, err_buf, ERR_BUF_LEN));
        ret = -1;
        goto out;
    }

    log_debug("read cmds number [%d]", cur_line);

out:
    if (fp) {
        fclose(fp);
        fp = NULL;
    }
    if (ret != 0) {
        for (map<uint32_t, cmd_msg_t*>::iterator it = cmd_conf_map->begin(); it != cmd_conf_map->end(); ++ it) {
            if (it->second) {
                cmd_msg_free(it->second);
                it->second = NULL;
            }
        }
    }
    return ret;
}

pkg_ctx_t *pkg_ctx_new(global_pkg_ctx_t *g_ctx) {
    // only called in one thread please, do not share
    if (g_ctx == NULL) {
        log_warning("global_pkg_ctx must inited before pkg_ctx");
        return NULL;
    }
    pb_ctx_t *pb_ctx = g_ctx->cur;

    pkg_ctx_t *ctx = NULL;
    char err_buf[ERR_BUF_LEN];
    map<uint32_t, cmd_msg_t*> cmd_table;
    std::pair<map<uint32_t, cmd_msg_t*>::iterator, bool> ret;

    ctx = (pkg_ctx_t*)calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        log_warning("fail to calloc, oom?");
        goto errout;
    }
    ctx->pb_ctx = pb_ctx;

    //ctx->cmd_table = pb_ctx->cmd_conf_map;
    // return ctx;
    // atomic_inc(&(pb_ctx->count));

    for (map<uint32_t, cmd_msg_t*>::iterator it = pb_ctx->cmd_conf_map->begin(); it != pb_ctx->cmd_conf_map->end(); ++ it) {
        ret = cmd_table.insert(std::pair<uint32_t, cmd_msg_t*>(it->first, NULL));
        if (ret.second == false) {
            log_warning("element already exists, should not appear");
            goto errout;
        }
        cmd_msg_t *cmsg = (cmd_msg_t*)calloc(1, sizeof(cmd_msg_t));
        if (cmsg == NULL) {
            log_warning("fail to calloc, oom?");
            goto errout;
        }
        ret.first->second = cmsg;

        cmsg->cmd_no = it->first;
        cmsg->cmd_name = strdup(it->second->cmd_name);
        if (cmsg->cmd_name == NULL) {
            log_warning("fail to strdup, oom?");
            goto errout;
        }

        cmsg->ups_msg = it->second->ups_msg->New();
        cmsg->downs_msg = it->second->downs_msg->New();
        if (!cmsg->ups_msg || !cmsg->downs_msg) {
            log_warning("fail to create ups/downs msg");
            goto errout;
        }
    }

    ctx->cmd_table = new std::map< uint32_t, cmd_msg_t*> ( cmd_table );

    /*
     *ctx->cmd_table = new bsl::readmap<uint32_t, cmd_msg_t*>();
     *if (ctx->cmd_table->assign(cmd_table.begin(), cmd_table.end()) != 0) {
     *    log_warning("fail to assign cmd_table to readman");
     *    goto errout;
     *}
     */

    log_notice("success create pkg_ctx");
    goto out;

errout:
    if (ctx && ctx->cmd_table == NULL) {
        for (map<uint32_t, cmd_msg_t*>::iterator it = cmd_table.begin(); it != cmd_table.end(); ++ it) {
            if (it->second) {
                cmd_msg_free(it->second);
                it->second = NULL;
            }
        }
    }
    if (ctx) {
        pkg_ctx_free(ctx);
        ctx = NULL;
    }
out:
    return ctx;
}

int pkg_ctx_free(pkg_ctx_t *ctx) {
    if (ctx) {
        // atomic_dec(&(ctx->pb_ctx->count));
        ctx->pb_ctx = NULL;
        if (ctx->cmd_table)
        {
            for (std::map<uint32_t, cmd_msg_t*>::iterator it = ctx->cmd_table->begin(); it != ctx->cmd_table->end(); ++ it) 
            {
                if (it->second) {
                    cmd_msg_free(it->second);
                    it->second = NULL;
                }
            }

            delete ctx->cmd_table;
            ctx->cmd_table = NULL;
        }
        // ctx->cmd_table = NULL;
        free(ctx);
    }
    return 0;
}

int pkg_msgpack2pb(pkg_ctx_t *ctx, unsigned char *data_buf, int *data_len, int data_buf_len, uint32_t cmd_no, int pkg_stream) {
    if (!ctx || !data_buf) {
        log_warning("ctx and data_buf cannot be NULL");
        return -1;
    }
    if (*data_len == 0) {
        return 0;
    }

    int ret = 0;
    Message *msg = NULL;
    cmd_msg_t *cmsg = NULL;

    std::map<uint32_t, cmd_msg_t*>::iterator it = ctx->cmd_table->find(cmd_no);
    if( it == ctx->cmd_table->end())
    {
        log_warning("cannot find cmd: %d info ", cmd_no);
        return -1;
    }
    else
    {
        cmsg = it->second;
    }

    /*
     *ret = ctx->cmd_table->get(cmd_no, &cmsg);
     *if (ret == bsl::HASH_NOEXIST) {
     *    log_warning("unsupported cmd: %u", cmd_no);
     *    return -1;
     *}
     */

    if (pkg_stream == PKG_UPSTREAM) {
        msg = cmsg->ups_msg;
    } else {
        msg = cmsg->downs_msg;
    }

    ret = msgpack2pb(msg, data_buf, *data_len);
    if (ret != 0) {
        log_warning("fail to msgpack2pb");
        return -1;
    }

    *data_len = msg->ByteSize();
    if (msg->SerializeToArray(data_buf, data_buf_len) == false) {
        log_warning("pb fail to serialize to array, buf_size: %d, required size: %d", data_buf_len, *data_len);
        return -1;
    }

    return 0;
}

int pkg_pb2msgpack(pkg_ctx_t *ctx, unsigned char *data_buf, int *data_len, int data_buf_len, uint32_t cmd_no, int pkg_stream) {
    if (!ctx || !data_buf) {
        log_warning("ctx and data_buf cannot be NULL");
        return -1;
    }
    if (*data_len == 0) {
        return 0;
    }

    int ret = 0;
    Message *msg = NULL;
    cmd_msg_t *cmsg = NULL;

    /*
     *ret = ctx->cmd_table->get(cmd_no, &cmsg);
     *if (ret == bsl::HASH_NOEXIST) {
     *    log_warning("unsupported cmd: %u", cmd_no);
     *    return -1;
     *}
     */

    std::map<uint32_t, cmd_msg_t*>::iterator it = ctx->cmd_table->find(cmd_no);
    if( it == ctx->cmd_table->end())
    {
        log_warning("cannot find cmd: %d info ", cmd_no);
        return -1;
    }
    else
    {
        cmsg = it->second;
    }

    if (pkg_stream == PKG_UPSTREAM) {
        msg = cmsg->ups_msg;
    } else {
        msg = cmsg->downs_msg;
    }

    if (msg->ParseFromArray(data_buf, *data_len) == false) {
        log_warning("pb fail to parse from array, cmd_no: %d, data_len: %d", cmd_no, *data_len);
        return -1;
    }

    /*
     *std::string str_res;
     *TextFormat::PrintToString(*msg, & str_res);
     *printf("\n\n --------------RES ----------test ...\n");
     *printf("%s\n",str_res.c_str() );
     *printf(" test ...\n");
     */

    *data_len = data_buf_len;
    ret = pb2msgpack(msg, data_buf, data_len);
    if (ret != 0) {
        log_warning("fail to pb2msgpack");
        return -1;
    }

    return 0;
}

static int msgpack2pb(Message *msg, const unsigned char *msgpack, int msgpack_len) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    msg->Clear();

    int ret = 0;

    msgpack_unpacked u_msg;
    msgpack_unpacked_init(&u_msg);

    if (!msgpack_unpack_next(&u_msg, (const char*)msgpack, msgpack_len, NULL)) {
        log_warning("fail to unpack msgpack");
        goto out;
    }

    ret = parse_msgpack_object(msg, u_msg.data);
    if (ret != 0) {
        log_warning("fail to convert msgpack to pb: %s", msg->GetTypeName().c_str());
        goto out;
    }
out:
    msgpack_unpacked_destroy(&u_msg);
    return ret;
}

static int parse_msgpack_object_repeated(Message *msg, const Reflection *msg_refl, const FieldDescriptor *msg_fd, msgpack_object o) {
    const EnumValueDescriptor *msg_evd = NULL;

    if (o.type != MSGPACK_OBJECT_ARRAY) {
        log_warning("invalid value type for field: %s, array required, now is: %d", msg_fd->full_name().c_str(), o.type);
        return -1;
    }
    char str_buf[STR_BUF_LEN + 1] = {0}; // for im, php str2x
    int size = o.via.array.size;
    msgpack_object *cur = o.via.array.ptr;
    switch (msg_fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->AddInt32(msg, msg_fd, (int32_t)(cur->via.u64));
                } else if (cur->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_refl->AddInt32(msg, msg_fd, (int32_t)(cur->via.i64));
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddInt32(msg, msg_fd, (int32_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->AddInt64(msg, msg_fd, (int64_t)(cur->via.u64));
                } else if (cur->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_refl->AddInt64(msg, msg_fd, (int64_t)(cur->via.i64));
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddInt64(msg, msg_fd, (int64_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->AddUInt32(msg, msg_fd, (uint32_t)(cur->via.u64));
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddUInt32(msg, msg_fd, (uint32_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, positive-integer required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->AddUInt64(msg, msg_fd, (uint64_t)(cur->via.u64));
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddUInt64(msg, msg_fd, (uint64_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, positive-integer required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_DOUBLE) {
                    msg_refl->AddDouble(msg, msg_fd, cur->via.dec);
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddDouble(msg, msg_fd, (double)(strtold(str_buf, NULL)));
                } else {
                    log_warning("invalid value type for field: %s, double required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_DOUBLE) {
                    msg_refl->AddFloat(msg, msg_fd, (float)(cur->via.dec));
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_refl->AddFloat(msg, msg_fd, (float)(strtold(str_buf, NULL)));
                } else {
                    log_warning("invalid value type for field: %s, float required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_BOOLEAN) {
                    msg_refl->AddBool(msg, msg_fd, cur->via.boolean);
                } else {
                    log_warning("invalid value type for field: %s, boolean required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)cur->via.u64);
                } else if (cur->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)cur->via.i64);
                } else if (cur->type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->via.raw.ptr, cur->via.raw.size);
                    str_buf[cur->via.raw.size] = '\0';
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
                if (msg_evd == NULL) {
                    log_warning("invalid value type for field: %s, integer(enum) required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                } else {
                    msg_refl->AddEnum(msg, msg_fd, msg_evd);
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_RAW) {
                    msg_refl->AddString(msg, msg_fd, string(cur->via.raw.ptr, cur->via.raw.size));
                } else if (cur->type == MSGPACK_OBJECT_NIL) {
                    msg_refl->AddString(msg, msg_fd, string());
                } else {
                    log_warning("invalid value type for field: %s, string(raw) required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            for (int i = 0; i < size; i ++, cur ++) {
                if (cur->type == MSGPACK_OBJECT_MAP) {
                    Message *sub_msg = msg_refl->AddMessage(msg, msg_fd, NULL);
                    if (parse_msgpack_object(sub_msg, *cur) != 0) {
                        return -1;
                    }
                } else if (cur->type == MSGPACK_OBJECT_ARRAY && cur->via.array.size == 0 && !msg_fd->is_required()) {
                    // do nothing
                    // php empty array
                } else {
                    log_warning("invalid value type for field: %s, map(message) required, cur is: %d", msg_fd->full_name().c_str(), cur->type);
                    return -1;
                }
            }
            break;
        default:
            log_warning("invalid value type for field: %s, unsupported", msg_fd->full_name().c_str());
            return -1;
            break;
    }
    return 0;
}

static int parse_msgpack_object(Message *msg, msgpack_object o) {
    const Descriptor *msg_des = msg->GetDescriptor();
    const Reflection *msg_refl = msg->GetReflection();
    const FieldDescriptor *msg_fd = NULL;
    const EnumValueDescriptor *msg_evd = NULL;

    char str_buf[STR_BUF_LEN] = {0};

    if (msg_des == NULL || msg_refl == NULL) {
        log_warning("fail to get descriptor or reflection for msg: %s", msg->GetTypeName().c_str());
        return -1;
    }
    if (o.type != MSGPACK_OBJECT_MAP) {
        log_warning("invalid msgpack object type to parse, map required for msg: %s, now is: %d", msg->GetTypeName().c_str(), o.type);
        return -1;
    }
    if (o.via.map.size == 0) {
        // FIXME: ?
        log_warning("empty map for msg: %s", msg->GetTypeName().c_str());
        return -1;
    }
    msgpack_object_kv *start = o.via.map.ptr;
    msgpack_object_kv *end = o.via.map.ptr + o.via.map.size;
    msgpack_object_kv *cur = start;
    for( ; cur < end; cur ++) {
        if (cur->key.type != MSGPACK_OBJECT_RAW) {
            log_warning("invalid key type, string(raw) required");
            return -1;
        }
        msg_fd = msg_des->FindFieldByName(string(cur->key.via.raw.ptr, cur->key.via.raw.size));
        if (msg_fd == NULL) {
            log_debug("no field found in msg. field: [%s], msg: [%s]. Dropped!", string(cur->key.via.raw.ptr, cur->key.via.raw.size).c_str(), msg->GetTypeName().c_str());
            //return -1;
            continue;
        }

        if (msg_fd->is_repeated()) {
            if (parse_msgpack_object_repeated(msg, msg_refl, msg_fd, cur->val) != 0) {
                return -1;
            }
            continue;
        }

        switch (msg_fd->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                if (cur->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->SetInt32(msg, msg_fd, (int32_t)(cur->val.via.u64));
                } else if (cur->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_refl->SetInt32(msg, msg_fd, (int32_t)(cur->val.via.i64));
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetInt32(msg, msg_fd, (int32_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                if (cur->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->SetInt64(msg, msg_fd, (int64_t)(cur->val.via.u64));
                } else if (cur->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_refl->SetInt64(msg, msg_fd, (int64_t)(cur->val.via.i64));
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetInt64(msg, msg_fd, (int64_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                if (cur->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->SetUInt32(msg, msg_fd, (uint32_t)(cur->val.via.u64));
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetUInt32(msg, msg_fd, (uint32_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, positive-integer required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                if (cur->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_refl->SetUInt64(msg, msg_fd, (uint64_t)(cur->val.via.u64));
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetUInt64(msg, msg_fd, (uint32_t)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, positive-integer required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                if (cur->val.type == MSGPACK_OBJECT_DOUBLE) {
                    msg_refl->SetDouble(msg, msg_fd, cur->val.via.dec);
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetDouble(msg, msg_fd, (double)(strtold(str_buf, NULL)));
                } else {
                    log_warning("invalid value type for field: %s, double required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                if (cur->val.type == MSGPACK_OBJECT_DOUBLE) {
                    msg_refl->SetFloat(msg, msg_fd, (float)(cur->val.via.dec));
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_refl->SetFloat(msg, msg_fd, (float)(strtold(str_buf, NULL)));
                } else {
                    log_warning("invalid value type for field: %s, float required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                if (cur->val.type == MSGPACK_OBJECT_BOOLEAN) {
                    msg_refl->SetBool(msg, msg_fd, cur->val.via.boolean);
                } else {
                    log_warning("invalid value type for field: %s, boolean required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                if (cur->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)cur->val.via.u64);
                } else if (cur->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)cur->val.via.i64);
                } else if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    // NOTE:
                    // Adding this just for tb-im, may have problem when conversation
                    // Really not prefer it
                    // by cdz
                    if (cur->val.via.raw.size > STR_BUF_LEN) {
                        log_warning("str too long: %d", cur->val.via.raw.size);
                        return -1;
                    } 
                    memcpy(str_buf, cur->val.via.raw.ptr, cur->val.via.raw.size);
                    str_buf[cur->val.via.raw.size] = '\0';
                    msg_evd = msg_fd->enum_type()->FindValueByNumber((int)(strtoll(str_buf, NULL, 10)));
                } else {
                    log_warning("invalid value type for field: %s, integer required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                if (msg_evd == NULL) {
                    log_warning("invalid value type for field: %s, integer(enum) required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                } else {
                    msg_refl->SetEnum(msg, msg_fd, msg_evd);
                }
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                if (cur->val.type == MSGPACK_OBJECT_RAW) {
                    msg_refl->SetString(msg, msg_fd, string(cur->val.via.raw.ptr, cur->val.via.raw.size));
                } else if (cur->val.type == MSGPACK_OBJECT_NIL) {
                    msg_refl->SetString(msg, msg_fd, string());
                } else {
                    log_warning("invalid value type for field: %s, string(raw) required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                if (cur->val.type == MSGPACK_OBJECT_MAP) {
                    Message *sub_msg = msg_refl->MutableMessage(msg, msg_fd, NULL);
                    if (parse_msgpack_object(sub_msg, cur->val) != 0) {
                        return -1;
                    }
                } else if (cur->val.type == MSGPACK_OBJECT_ARRAY && cur->val.via.array.size == 0 && !msg_fd->is_required()) {
                    // do nothing
                    // php empty array
                } else {
                    log_warning("invalid value type for field: %s, map(message) required, now is: %d", msg_fd->full_name().c_str(), cur->val.type);
                    return -1;
                }
                break;
            default:
                log_warning("invalid value type for field: %s, unsupported", msg_fd->full_name().c_str());
                return -1;
                break;
        }
    }
    return 0;
}

static int pb2msgpack(Message *msg, unsigned char *msgpack_buf, int *msgpack_buf_len) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int ret = 0;

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    ret = parse_msg(msg, &pk);
    if (ret != 0) {
        log_warning("fail to parse pb to msgpack, msg: %s", msg->GetTypeName().c_str());
        goto out;
    }

    if (*msgpack_buf_len < sbuf.size) {
        log_warning("msgpack buf len is too small, need: %d", sbuf.size);
        ret = -1;
        goto out;
    }

    memcpy(msgpack_buf, sbuf.data, sbuf.size);
    *msgpack_buf_len = sbuf.size;
out:
    msgpack_sbuffer_destroy(&sbuf);
    return ret;
}

static int parse_msg_repeated(const Message *msg, msgpack_packer *pk, const Reflection * refl,const FieldDescriptor *field) {
    size_t count = refl->FieldSize(*msg,field);
    msgpack_pack_array(pk, count);

    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_DOUBLE:
            for(size_t i = 0 ; i != count ; ++i) {
                double	value1 = refl->GetRepeatedDouble(*msg,field,i);
                msgpack_pack_double(pk, value1);
            }
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            for(size_t i = 0 ; i != count ; ++i) {
                float value2 = refl->GetRepeatedFloat(*msg,field,i);
                msgpack_pack_float(pk, value2);
            }
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            for(size_t i = 0 ; i != count ; ++i) {
                int64_t value3 = refl->GetRepeatedInt64(*msg,field,i);
                msgpack_pack_int64(pk, value3);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            for(size_t i = 0 ; i != count ; ++i) {
                uint64_t value4 = refl->GetRepeatedUInt64(*msg,field,i);
                msgpack_pack_uint64(pk, value4);
            }
            break;
        case FieldDescriptor::CPPTYPE_INT32:
            for(size_t i = 0 ; i != count ; ++i) {
                int32_t value5 = refl->GetRepeatedInt32(*msg,field,i);
                msgpack_pack_int32(pk, value5);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            for(size_t i = 0 ; i != count ; ++i) {
                uint32_t value6 = refl->GetRepeatedUInt32(*msg,field,i);
                msgpack_pack_uint32(pk, value6);
            }
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            for(size_t i = 0 ; i != count ; ++i) {
                bool value7 = refl->GetRepeatedBool(*msg,field,i);
                value7 ? msgpack_pack_true(pk) : msgpack_pack_false(pk);
            }
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            for(size_t i = 0 ; i != count ; ++i) {
                string value8 = refl->GetRepeatedString(*msg,field,i);
                msgpack_pack_raw(pk, value8.length());
                msgpack_pack_raw_body(pk, value8.data(), value8.length());
            }
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            for(size_t i = 0 ; i != count ; ++i) {
                const Message *value9 = &(refl->GetRepeatedMessage(*msg,field,i));
                if (parse_msg(value9, pk) != 0) {
                    return -1;
                }
            }
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            for(size_t i = 0 ; i != count ; ++i) {
                const EnumValueDescriptor* value10 = refl->GetRepeatedEnum(*msg,field,i);
                msgpack_pack_int(pk, value10->number());
            }
            break;
        default:
            log_warning("unsupported field type for field: %s", field->full_name().c_str());
            return -1;
            break;
    }
    return 0;
}

static int parse_msg(const Message *msg, msgpack_packer *pk) {
    const Descriptor *d = msg->GetDescriptor();
    if (!d) {
        log_warning("fail to get descriptor for msg: %s", msg->GetTypeName().c_str());
        return -1;
    }

    const Reflection *refl = msg->GetReflection();
    if (!refl) {
        log_warning("fail to get reflection for msg: %s", msg->GetTypeName().c_str());
        return -1;
    }

    vector< const FieldDescriptor * > fd_list;
    refl->ListFields(*msg, &fd_list);

    size_t count = fd_list.size();
    msgpack_pack_map(pk, count);

    double value1;
    float value2;
    int64_t value3;
    uint64_t value4;
    int32_t value5;
    uint32_t value6;
    bool value7;
    string value8;
    const Message *value9;
    const EnumValueDescriptor *value10;

    for (size_t i = 0; i < count ; ++i) {
        const FieldDescriptor *field = fd_list[i];
        if (!field) {
            log_warning("fail to get field descriptor for msg: %s", msg->GetTypeName().c_str());
            return -1;
        }
        const char *name = field->name().c_str();
        size_t str_len = strlen(name);
        msgpack_pack_raw(pk, str_len);
        msgpack_pack_raw_body(pk, name, str_len);

        if (field->is_repeated()) {
            if (parse_msg_repeated(msg, pk, refl, field) != 0) {
                return -1;
            }
            continue;
        }

        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_DOUBLE:
                value1 = refl->GetDouble(*msg,field);
                msgpack_pack_double(pk, value1);
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                value2 = refl->GetFloat(*msg,field);
                msgpack_pack_float(pk, value2);
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                value3 = refl->GetInt64(*msg,field);
                msgpack_pack_int64(pk, value3);
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                value4 = refl->GetUInt64(*msg,field);
                msgpack_pack_uint64(pk, value4);
                break;
            case FieldDescriptor::CPPTYPE_INT32:
                value5 = refl->GetInt32(*msg,field);
                msgpack_pack_int32(pk, value5);
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                value6 = refl->GetUInt32(*msg,field);
                msgpack_pack_uint32(pk, value6);
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                value7 = refl->GetBool(*msg,field);
                value7 ? msgpack_pack_true(pk) : msgpack_pack_false(pk);
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                value8 = refl->GetString(*msg,field);
                msgpack_pack_raw(pk, value8.length());
                msgpack_pack_raw_body(pk, value8.data(), value8.length());
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                value9 = &(refl->GetMessage(*msg,field));
                if (parse_msg(value9, pk) != 0) {
                    return -1;
                }
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                value10 = refl->GetEnum(*msg,field);
                msgpack_pack_int(pk, value10->number());
                break;
            default:
                log_warning("unsupported field type for field: %s", field->full_name().c_str());
                return -1;
                break;
        }
    }
    return 0;
}






/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
