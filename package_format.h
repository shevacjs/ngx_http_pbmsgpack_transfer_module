/***************************************************************************
 * 
 * Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file package_format.h
 * @author chendazhuang(com@baidu.com)
 * @date 2013/11/20 10:23:04
 * @brief 
 *  
 **/




#ifndef  __PACKAGE_FORMAT_H_
#define  __PACKAGE_FORMAT_H_

#include <map>
#include <asm/atomic.h>

// #include <bsl/containers/hash/bsl_readmap.h>
#include <google/protobuf/message.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

#include <google/protobuf/text_format.h>

#define PKG_UPSTREAM 1
#define PKG_DOWNSTREAM 2

typedef struct _cmd_msg_t {
    uint32_t cmd_no;
    char *cmd_name;
    char *cmd_base_name;
    google::protobuf::Message *ups_msg;
    google::protobuf::Message *downs_msg;
} cmd_msg_t;

struct _global_pkg_ctx_t;
struct _pb_ctx_t;

typedef struct _pkg_ctx_t {
    // bsl::readmap<uint32_t, cmd_msg_t*> *cmd_table;
    std::map<uint32_t, cmd_msg_t*> * cmd_table;
    struct _pb_ctx_t *pb_ctx;
} pkg_ctx_t;

typedef struct _pb_ctx_t {
    std::map<uint32_t, cmd_msg_t*> *cmd_conf_map;
    google::protobuf::compiler::DiskSourceTree *source_tree;
    google::protobuf::compiler::MultiFileErrorCollector *error_collector;
    google::protobuf::compiler::Importer *importer;
    const google::protobuf::DescriptorPool *des_pool;
    google::protobuf::DynamicMessageFactory *msg_factory;
    // atomic_t count;
} pb_ctx_t;

typedef struct _global_pkg_ctx_t {
//    pb_ctx_t *cur, *old;
//    bool reload_done;
    pb_ctx_t* cur;
} global_pkg_ctx_t;

int init_global_pkg_ctx(char *proto_conf_file);
int global_pkg_ctx_free();

pkg_ctx_t *pkg_ctx_new(global_pkg_ctx_t *g_ctx);
int pkg_ctx_free(pkg_ctx_t *ctx);

pb_ctx_t *pb_ctx_new();
int pb_ctx_init(pb_ctx_t *pb_ctx, char *proto_conf_file);
void pb_ctx_deinit(pb_ctx_t *pb_ctx);
void pb_ctx_free(pb_ctx_t *pb_ctx);

int pkg_msgpack2pb(pkg_ctx_t *ctx, unsigned char *data_buf, int *data_len, int data_buf_len, uint32_t cmd_no, int pkg_stream);
int pkg_pb2msgpack(pkg_ctx_t *ctx, unsigned char *data_buf, int *data_len, int data_buf_len, uint32_t cmd_no, int pkg_stream);

extern global_pkg_ctx_t *g_pkg_ctx;







#endif  //__PACKAGE_FORMAT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
