/***************************************************************************
 *
 * Copyright (c) 2011 Baidu.com, Inc. All Rights Reserved
 * $Id$
 *
 **************************************************************************/



/**
 * @file ngx_http_pb_msgpack_transfer_module.c
 * @author forum(chenjiansen@baidu.com)
 * @date 2014/03/11 23:30
 * @version $Revision$
 * @brief
 *
 **/
// stub module to test header files' C++ compatibilty

extern "C" 
{

  #include <ngx_config.h>
  #include <ngx_core.h>
  #include <ngx_event.h>
  #include <ngx_event_connect.h>
  #include <ngx_event_pipe.h>

  #include <ngx_http.h>
}

#include "package_format.h"

static const int DEFAULT_STR_BUF_SIZE = 20;

static pkg_ctx_t* g_ctx = NULL;

// global buffer size
static int g_pbmsgpack_buf_size = 16 * 1024;
static const int g_max_content_type_size = 1024;

static ngx_str_t g_data_type_header = ngx_string("http_x_bd_data_type");
static ngx_str_t g_out_data_type_header = ngx_string("sent_http_x_bd_data_type");
static ngx_str_t g_protobuf_str = ngx_string("protobuf");
static ngx_str_t g_msgpack_str = ngx_string("msgpack");
static ngx_str_t g_msgpack_nginx_str = ngx_string("msgpack,nginx");
static ngx_str_t g_protobuf_nginx_str = ngx_string("protobuf,nginx");
static ngx_str_t g_proto_conf_path = ngx_string("./conf/protos");
static ngx_str_t g_content_type_str = ngx_string("http_content_type");

static const char* const g_bound_str = "boundary=";
static const char* const g_multipart_form = "multipart/form-data";
static const char* const g_tag_name = "\"data";

typedef struct _ngx_http_pb_msgpack_transfer_loc_conf_t
{
	ngx_flag_t		pbmsgpack_transfer_flag;
	ngx_flag_t		pbmsgpack_filter_flag;
}ngx_http_pb_msgpack_transfer_loc_conf_t;

typedef struct _ngx_http_pb_msgpack_transfer_main_conf_t
{
	ngx_flag_t module_flag;
	ngx_str_t proto_conf_path;
	ngx_int_t pbmsgpack_buf_size;
}ngx_http_pb_msgpack_transfer_main_conf_t;

typedef struct _ngx_http_pbmsgpack_transfer_ctx_t
{
	ngx_int_t is_waiting_body;

	ngx_int_t is_header_illegal;
	ngx_table_elt_t* data_type_header;

	ngx_int_t has_filter_deal;
	ngx_table_elt_t* sent_data_type_header;

	ngx_table_elt_t* content_type_header;

	u_char* pb_buf;
	ngx_uint_t pb_buf_pos;
	ngx_uint_t pb_buf_size;
	u_char* msgpack_buf;
	ngx_uint_t msgpack_pos;
	ngx_uint_t msgpack_buf_size;
	ngx_uint_t transfer_cost;
	ngx_uint_t filter_cost;
}ngx_http_pbmsgpack_transfer_ctx_t;

enum PBMSGPACK_VAR_TYPE
{
	PB_VAR_TYPE_TRANSFER_COST = 0,
	PB_VAR_TYPE_FILTER_COST,
	PB_VAR_TYPE_TRANSFER_SIZE ,
	PB_VAR_TYPE_FILTER_SIZE,
};

static ngx_command_t ngx_http_pb_msgpack_transfer_commands[] =
{

	{
		ngx_string("pbmsgpack_flag"),
		NGX_HTTP_MAIN_CONF |  NGX_CONF_TAKE1 ,
		ngx_conf_set_flag_slot,
		NGX_HTTP_MAIN_CONF_OFFSET,
		offsetof(ngx_http_pb_msgpack_transfer_main_conf_t,module_flag),
		NULL,
	},

	{
		ngx_string("pbmsgpack_conf_path"),
		NGX_HTTP_MAIN_CONF |  NGX_CONF_TAKE1 ,
		ngx_conf_set_str_slot,
		NGX_HTTP_MAIN_CONF_OFFSET,
		offsetof(ngx_http_pb_msgpack_transfer_main_conf_t,proto_conf_path),
		NULL,
	},

	{
		ngx_string("pbmsgpack_buf_size"),
		NGX_HTTP_MAIN_CONF |  NGX_CONF_TAKE1 ,
		ngx_conf_set_size_slot,
		NGX_HTTP_MAIN_CONF_OFFSET,
		offsetof(ngx_http_pb_msgpack_transfer_main_conf_t,pbmsgpack_buf_size),
		NULL,
	},

	{
		ngx_string("pbmsgpack_transfer_flag"),
		NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1 ,
		ngx_conf_set_flag_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_pb_msgpack_transfer_loc_conf_t,pbmsgpack_transfer_flag),
		NULL,
	},

	{
		ngx_string("pbmsgpack_filter_flag"),
		NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1 ,
		ngx_conf_set_flag_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_pb_msgpack_transfer_loc_conf_t,pbmsgpack_filter_flag),
		NULL,
	},

	ngx_null_command
};


static ngx_int_t ngx_http_pb_msgpack_transfer_init(ngx_conf_t* cf);

static void* ngx_http_pb_msgpack_transfer_create_loc_conf(ngx_conf_t* cf);
static void* ngx_http_pb_msgpack_transfer_create_main_conf(ngx_conf_t* cf);

static ngx_int_t ngx_http_pb_msgpack_transfer_handler(ngx_http_request_t* r);

static void ngx_http_transfer_body(ngx_http_request_t* r);

static void ngx_http_pb_msgpack_exit_worker(ngx_cycle_t* cycle);

static char* ngx_http_pb_msgpack_transfer_merge_loc_conf(ngx_conf_t* cf, void* parent, void * child);

static ngx_int_t ngx_http_pb_msgpack_body_filter(ngx_http_request_t* r , ngx_chain_t* in);
static ngx_int_t ngx_http_pb_msgpack_header_filter(ngx_http_request_t* r );

static ngx_table_elt_t* ngx_http_pbmsgpack_get_header(ngx_http_request_t* r, ngx_str_t* str_header);
static ngx_table_elt_t* ngx_http_pbmsgpack_get_res_header(ngx_http_request_t* r, ngx_str_t* str_header);

static int get_multipart_form_header_info(ngx_http_request_t* r, ngx_table_elt_t* elt, char* bound);
static int get_multipart_form_info(char* info, int size,const char* bound, const char* tag_name, int& start_pos, int& end_pos);

static ngx_int_t ngx_http_pbmsgpack_add_variables(ngx_conf_t *cf);


static ngx_int_t ngx_http_pbmsgpack_var(ngx_http_request_t *r,ngx_http_variable_value_t *v, uintptr_t data);

static unsigned long int ngx_http_pbmsgpack_get_time_us();

static ngx_http_output_header_filter_pt ngx_http_next_header_filter;
static ngx_http_output_body_filter_pt ngx_http_next_body_filter;



static ngx_http_variable_t ngx_http_pbmsgpack_vars[] = 
{
	{ ngx_string("pbmsgpack_transfer_cost"),NULL,ngx_http_pbmsgpack_var,
		PB_VAR_TYPE_TRANSFER_COST, NGX_HTTP_VAR_NOHASH, 0 },
	{ ngx_string("pbmsgpack_filter_cost"),NULL,ngx_http_pbmsgpack_var,
		PB_VAR_TYPE_FILTER_COST, NGX_HTTP_VAR_NOHASH, 0 },
	{	ngx_string("pbmsgpack_transfer_size"),NULL,ngx_http_pbmsgpack_var,
		PB_VAR_TYPE_TRANSFER_SIZE, NGX_HTTP_VAR_NOHASH, 0 },
	{	ngx_string("pbmsgpack_filter_size"),NULL,ngx_http_pbmsgpack_var,
		PB_VAR_TYPE_FILTER_SIZE, NGX_HTTP_VAR_NOHASH, 0 },
    { ngx_null_string, NULL, NULL, 0, 0, 0 }
};

static ngx_http_module_t ngx_http_pb_msgpack_transfer_module_ctx = 
{
	ngx_http_pbmsgpack_add_variables,											/* preconfiguration */
	ngx_http_pb_msgpack_transfer_init,				/* postconfiguration */

	ngx_http_pb_msgpack_transfer_create_main_conf, /* create main configuration */
	NULL,									/* init main configuration */

	NULL,	/* create server configuration */
	NULL,	/* merge server configuration */

	ngx_http_pb_msgpack_transfer_create_loc_conf,  /* create location configration */
	ngx_http_pb_msgpack_transfer_merge_loc_conf /* merge location configration */
};


ngx_module_t  ngx_http_pb_msgpack_transfer_module = 
{
	NGX_MODULE_V1,
	&ngx_http_pb_msgpack_transfer_module_ctx,			/* module context */
	ngx_http_pb_msgpack_transfer_commands,              /* module directives */
	NGX_HTTP_MODULE,							/* module type */
	NULL,										/* init master */
	NULL,										/* init module */
	NULL,										/* init process */
	NULL,										/* init thread */
	NULL,										/* exit thread */
	ngx_http_pb_msgpack_exit_worker,										/* exit process */
	NULL,										/* exit master */
	NGX_MODULE_V1_PADDING
};


static void* ngx_http_pb_msgpack_transfer_create_main_conf(ngx_conf_t* cf)
{

	ngx_http_pb_msgpack_transfer_main_conf_t* conf = NULL;
	conf = (ngx_http_pb_msgpack_transfer_main_conf_t*) ngx_pcalloc(cf->pool, sizeof(ngx_http_pb_msgpack_transfer_main_conf_t));
	if(NULL == conf)
	{
		return NULL;
	}

	conf->module_flag = NGX_CONF_UNSET;
	conf->pbmsgpack_buf_size = NGX_CONF_UNSET;
	conf->proto_conf_path.data  = NULL;
	conf->proto_conf_path.len = NULL;

	return conf;
}

static void* ngx_http_pb_msgpack_transfer_create_loc_conf(ngx_conf_t* cf)
{

	ngx_http_pb_msgpack_transfer_loc_conf_t* loc_conf = NULL;
	loc_conf = (ngx_http_pb_msgpack_transfer_loc_conf_t*) ngx_pcalloc(cf->pool, sizeof(ngx_http_pb_msgpack_transfer_loc_conf_t));

	// pcalloc make
	// loc_conf->pbmsgpack_transfer_flag = 0
	if(NULL == loc_conf)
	{
		return NULL;
	}

	loc_conf->pbmsgpack_transfer_flag = NGX_CONF_UNSET;
	loc_conf->pbmsgpack_filter_flag = NGX_CONF_UNSET;

	return loc_conf;
}


static ngx_int_t ngx_http_pb_msgpack_transfer_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;
	ngx_http_pb_msgpack_transfer_main_conf_t* pmtmcf;

	pmtmcf = (ngx_http_pb_msgpack_transfer_main_conf_t*) ngx_http_conf_get_module_main_conf(cf, ngx_http_pb_msgpack_transfer_module);

	// default conf path
	if(NULL == pmtmcf->proto_conf_path.data)
	{
		pmtmcf->proto_conf_path = g_proto_conf_path;
	}

	// default buffer size
	if(NGX_CONF_UNSET == pmtmcf->pbmsgpack_buf_size)
	{
		pmtmcf->pbmsgpack_buf_size = g_pbmsgpack_buf_size;
	}
	else
	{
		g_pbmsgpack_buf_size = pmtmcf->pbmsgpack_buf_size;
	}

	ngx_log_debug3(NGX_LOG_DEBUG_HTTP, cf->log, 0, "pbmsgpack main conf info! flag:%d, path:%V, size:%u ", pmtmcf->module_flag, & pmtmcf->proto_conf_path, pmtmcf->pbmsgpack_buf_size);


	if(NGX_CONF_UNSET == pmtmcf->module_flag || 0 == pmtmcf->module_flag)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "ngx_http_pb_msgpack_transfer_main_conf_t flag is 0, donot add hook !");
		return NGX_OK;
	}

	//设置钩子
    cmcf = (ngx_http_core_main_conf_t *)ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    h = (ngx_http_handler_pt *)ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
	if(NULL == h)
	{
        return NGX_ERROR;
	}

	*h = ngx_http_pb_msgpack_transfer_handler;

	// output filter
	ngx_http_next_body_filter = ngx_http_top_body_filter;
	ngx_http_top_body_filter = ngx_http_pb_msgpack_body_filter;

	ngx_http_next_header_filter = ngx_http_top_header_filter;
	ngx_http_top_header_filter = ngx_http_pb_msgpack_header_filter;


	if(pmtmcf->proto_conf_path.len >= 1024)
	{
		ngx_log_error(NGX_LOG_ALERT, cf->log, 0 , "the conf path is too long![path:%V]", & pmtmcf->proto_conf_path);
		return NGX_ERROR;
	}


	char pck_conf_path[1024];
	memcpy(pck_conf_path, pmtmcf->proto_conf_path.data, pmtmcf->proto_conf_path.len);
	pck_conf_path[pmtmcf->proto_conf_path.len] = '\0';

	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "nginx try to init pkg context");

	if(NULL != g_ctx)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "pbmsgpack  g_ctx is not NULL, free it!");
		pkg_ctx_free(g_ctx);
		g_ctx = NULL;
	}

	if(NULL != g_pkg_ctx)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "pbmsgpack g_pkg_ctx is not NULL, free it!");
		global_pkg_ctx_free();
		g_pkg_ctx = NULL;
	}

	// re-init protobuf-msgpack context
	ngx_int_t ret = init_global_pkg_ctx(pck_conf_path);
	if(0 != ret)
	{
		ngx_log_error(NGX_LOG_ALERT, cf->log, 0, "init_global_pkg_ctx failed![ret:%d]",ret);
		return NGX_ERROR;
	}

	if(NULL == g_ctx)
	{
		g_ctx = pkg_ctx_new(g_pkg_ctx);
		if(NULL == g_ctx)
		{
			ngx_log_error(NGX_LOG_ALERT, cf->log, 0, "pkg_ctx_new failed!");
			return NGX_ERROR;
		}
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_pb_msgpack_transfer_handler(ngx_http_request_t *r)
{
	ngx_int_t rc = 0;
	ngx_http_pb_msgpack_transfer_loc_conf_t* loc_conf = NULL;
	ngx_http_pbmsgpack_transfer_ctx_t* ctx = NULL;
	loc_conf = (ngx_http_pb_msgpack_transfer_loc_conf_t*) ngx_http_get_module_loc_conf(r, ngx_http_pb_msgpack_transfer_module);
	if(NULL == loc_conf )
	{
		return NGX_DECLINED;
	}

	if(0 == loc_conf->pbmsgpack_transfer_flag || NGX_CONF_UNSET == loc_conf->pbmsgpack_transfer_flag )
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP,r->connection->log,0, "pbmsgpack transfer flag is off or unset");
		return NGX_DECLINED;
	}
	
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pbmsgpack transfer try process body");

	ctx = (ngx_http_pbmsgpack_transfer_ctx_t*) ngx_http_get_module_ctx(r, ngx_http_pb_msgpack_transfer_module);

	if(NULL != ctx && 1 == ctx->is_header_illegal)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "head is illegal, skip it!");
		return NGX_DECLINED;
	}

	if(NULL == ctx)
	{
		ctx = (ngx_http_pbmsgpack_transfer_ctx_t* )ngx_pcalloc(r->pool, sizeof(ngx_http_pbmsgpack_transfer_ctx_t) );
		if(NULL == ctx)
		{
			return NGX_ERROR;
		}

		if(NULL == ctx->data_type_header)
		{
			//check whether transfer the protobuf to msgpack
			ngx_table_elt_t* data_type_elt = ngx_http_pbmsgpack_get_header(r, & g_data_type_header);
			ngx_table_elt_t* content_type_elt = ngx_http_pbmsgpack_get_header(r, & g_content_type_str);


			if( (NULL == data_type_elt
					|| data_type_elt->value.len != g_protobuf_str.len
					|| 0 != memcmp(data_type_elt->value.data, g_protobuf_str.data, g_protobuf_str.len)) 
					|| ( NULL == content_type_elt))
			{
				ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "unexpected request header! skip it!");
				if(NULL == data_type_elt)
				{
					ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pbmsgpack empty header unexpected request header! skip it!");
				}
				else
				{
					ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pbmsgpack get http_x_bd_data_type result:%V", & data_type_elt->value);
				}

				ctx->is_header_illegal = 1;
			}
			else
			{
				ctx->is_header_illegal = 0;
				ctx->data_type_header = data_type_elt;
				ctx->content_type_header = content_type_elt;
			}

		}

		ctx->is_waiting_body = 1;
		ngx_http_set_ctx(r, ctx, ngx_http_pb_msgpack_transfer_module);
	}
	else
	{
		if(NULL != ctx && 1 == ctx->is_header_illegal)
		{
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "head is illegal, skip it!");
			return NGX_DECLINED;
		}

		// if already read the whole request body, skip it
		if(0 == ctx->is_waiting_body)
		{
			return NGX_DECLINED;
		}
		else
		{
			return NGX_DONE;
		}

	}

	if(NULL != ctx && 1 == ctx->is_header_illegal)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "head is illegal, skip it!");
		return NGX_DECLINED;
	}

	// try read request body
	rc = ngx_http_read_client_request_body(r, ngx_http_transfer_body);

	if( NGX_ERROR ==rc || rc >= NGX_HTTP_SPECIAL_RESPONSE)
	{
		return rc;
	}
	else if(NGX_AGAIN == rc)
	{
		return NGX_DONE;
	}

	return NGX_DECLINED;
}

static ngx_table_elt_t* ngx_http_pbmsgpack_get_res_header(ngx_http_request_t* r, ngx_str_t* var)
{
	u_char            ch;
	ngx_uint_t        i, n;
	ngx_table_elt_t  *header;
	int prefix = sizeof("sent_http_") - 1;


	ngx_list_part_t* part = (ngx_list_part_t*)& r->headers_out.headers.part;
	header = (ngx_table_elt_t*)part->elts;

	for (i = 0; /* void */ ; i++) {

		if (i >= part->nelts) {
			if (part->next == NULL) {
				break;
			}

			part = part->next;
			header = (ngx_table_elt_t*)part->elts;
			i = 0;
		}

		if (header[i].hash == 0) {
			continue;
		}

		for (n = 0; n + prefix < var->len && n < header[i].key.len; n++) {
			ch = header[i].key.data[n];

			if (ch >= 'A' && ch <= 'Z') {
				ch |= 0x20;

			} else if (ch == '-') {
				ch = '_';
			}

			if (var->data[n + prefix] != ch) {
				break;
			}
		}

		if (n + prefix == var->len && n == header[i].key.len) {
			/*
			 *v->len = header[i].value.len;
			 *v->valid = 1;
			 *v->no_cacheable = 0;
			 *v->not_found = 0;
			 *v->data = header[i].value.data;
			 */

			return & header[i];

		}
	}


	return NULL;
}

static ngx_table_elt_t* ngx_http_pbmsgpack_get_header(ngx_http_request_t* r, ngx_str_t* var)
{
	u_char            ch;
	ngx_uint_t        i, n;
	ngx_table_elt_t  *header;
	int prefix = sizeof("http_") - 1;


	ngx_list_part_t* part = (ngx_list_part_t*)& r->headers_in.headers.part;
	header = (ngx_table_elt_t*)part->elts;

	for (i = 0; /* void */ ; i++) {

		if (i >= part->nelts) {
			if (part->next == NULL) {
				break;
			}

			part = part->next;
			header = (ngx_table_elt_t*)part->elts;
			i = 0;
		}

		if (header[i].hash == 0) {
			continue;
		}

		for (n = 0; n + prefix < var->len && n < header[i].key.len; n++) {
			ch = header[i].key.data[n];

			if (ch >= 'A' && ch <= 'Z') {
				ch |= 0x20;

			} else if (ch == '-') {
				ch = '_';
			}

			if (var->data[n + prefix] != ch) {
				break;
			}
		}

		if (n + prefix == var->len && n == header[i].key.len) {
			/*
			 *v->len = header[i].value.len;
			 *v->valid = 1;
			 *v->no_cacheable = 0;
			 *v->not_found = 0;
			 *v->data = header[i].value.data;
			 */

			return & header[i];

		}
	}


	return NULL;

}

static int ngx_http_pbmsgpack_get_cmd ( ngx_http_request_t* r)
{
	static ngx_str_t cmd_var_name = ngx_string("arg_cmd");
	ngx_http_variable_value_t* res_value;

	if(NULL == (res_value = ngx_http_get_variable(r,&cmd_var_name,0)))
	{
		return -1;
	}

	if(res_value->valid && 0 == res_value->not_found)
	{
		int cmd = ngx_atoi(res_value->data, res_value->len);
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "[pagecache] get varaibale success![cmd:%d]" , cmd);
		return cmd;
	}
	else
	{
		return -1;
	}

	return -1;

}

static void ngx_http_transfer_body(ngx_http_request_t* r)
{
	// already get the whole request body
	
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "enter ngx_http_transfer_body");
	unsigned long int start_time, end_time;
	start_time = ngx_http_pbmsgpack_get_time_us();

	// first, set ctx read request flag to 0
	ngx_http_pbmsgpack_transfer_ctx_t* ctx = NULL;
	ctx = (ngx_http_pbmsgpack_transfer_ctx_t*) ngx_http_get_module_ctx(r, ngx_http_pb_msgpack_transfer_module);
	if(NULL == ctx)
	{
		ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "[req_body_transfer] unexpected emmpty ctx!");
		ngx_http_finalize_request(r,NGX_DONE);
		return;
	}
	ctx->is_waiting_body = 0;

	// second, process data
	ngx_chain_t* bufs = r->request_body->bufs;
	if(NULL == bufs)
	{
		ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "request bufs is NULL!");
		ngx_http_finalize_request(r,NGX_DONE);
		return;
	}

	//try deal
	//first , get boundary info from header;
	
	char bound[g_max_content_type_size];
	int ret = get_multipart_form_header_info(r, ctx->content_type_header, bound);

	if(0 != ret)
	{
		ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "pb msgpack get_multipart_form_header_info failed!");
		ngx_http_finalize_request(r,NGX_DONE);
		return ;
	}

	// check cmd
	int cmd = ngx_http_pbmsgpack_get_cmd(r);
	if(cmd <= 0)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "illegal cmd is: %d", cmd);
		ngx_http_finalize_request(r,NGX_DONE);
		return;
	}

	ngx_int_t total_size = 0;
	ngx_int_t buf_num = 0;
	ngx_buf_t* temp_buf = NULL;
	ngx_chain_t* temp_chain = bufs;


	ctx->pb_buf_size = ctx->msgpack_buf_size = g_pbmsgpack_buf_size;
	ctx->pb_buf_pos = ctx->msgpack_pos = 0;
	ctx->pb_buf = (u_char*)ngx_pcalloc(r->pool, g_pbmsgpack_buf_size);
	// re-use pb buffer, 
	ctx->msgpack_buf =  ctx->pb_buf;

	while(NULL != temp_chain)
	{
		temp_buf = temp_chain->buf;
		if(NULL != temp_buf)
		{
			total_size += (int)(temp_buf->last - temp_buf->pos);
			++ buf_num;
		}

		temp_chain = temp_chain->next;
	}

	ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "[req_body_transfer]total requst body size:%d ,bufs:%d", total_size, buf_num);

	if(total_size >= g_pbmsgpack_buf_size)
	{
		//TODO
		ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "illegal request body size![now:%d][limit:%d]", total_size, g_pbmsgpack_buf_size);
		ngx_http_finalize_request(r,NGX_DONE);
		return;
	}


	temp_chain = bufs;
	while(NULL != temp_chain)
	{
		temp_buf = temp_chain->buf;
		if(NULL != temp_buf)
		{
			total_size = (int)(temp_buf->last - temp_buf->pos);
			memcpy(ctx->pb_buf + ctx->pb_buf_pos , temp_buf->pos, total_size);
			ctx->pb_buf_pos += total_size;
		}
		temp_chain = temp_chain->next;
	}

	//get the whole request body
	
	ctx->pb_buf[ ctx->pb_buf_pos ] = '\0';
	int data_start_pos;
	int data_end_pos;
	ret = get_multipart_form_info( (char*)ctx->pb_buf, ctx->pb_buf_pos,bound, g_tag_name, data_start_pos, data_end_pos);

	if(0 != ret)
	{
		ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "pb msgpack get_multipart_form_info failed!");
		ngx_http_finalize_request(r,NGX_DONE);
		return ;
	}

	// 节省空间，拷贝两次
	// 上面的pos是闭区间
		
	int temp_copy_size = ctx->pb_buf_pos - data_end_pos + 1;
	int temp_src_copy_pos = data_end_pos;
	int temp_dst_copy_pos = g_pbmsgpack_buf_size - temp_copy_size ;

	int raw_data_size = data_end_pos - data_start_pos ;
	int old_data_size = raw_data_size;

	memmove(ctx->pb_buf + temp_dst_copy_pos, ctx->pb_buf + temp_src_copy_pos, temp_copy_size );

	ngx_log_debug6(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack post body start:%d,end:%d,copy:%d,new_copy_pos:%d,old_data:%d,total:%d", data_start_pos, data_end_pos, temp_copy_size, temp_dst_copy_pos, old_data_size, ctx->pb_buf_pos);

	ret = pkg_pb2msgpack(g_ctx, ctx->pb_buf + data_start_pos, & raw_data_size, g_pbmsgpack_buf_size - temp_copy_size - data_start_pos, cmd, PKG_UPSTREAM);
	if(0 != ret)
	{
		ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "post body trans failed! pkg_pb2msgpack return failed!");
		ngx_http_finalize_request(r,NGX_DONE);
		return;
	}

	temp_src_copy_pos += raw_data_size - old_data_size;
	memmove(ctx->pb_buf + temp_src_copy_pos, ctx->pb_buf + temp_dst_copy_pos, temp_copy_size);

	ctx->pb_buf_pos += raw_data_size - old_data_size;

	ngx_log_debug6(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack post body start:%d,end:%d,copy:%d,new_copy_pos:%d,new_data:%d,total:%d", data_start_pos, data_end_pos, temp_copy_size, temp_dst_copy_pos, raw_data_size, ctx->pb_buf_pos);


	// first , reset the request body info
	ngx_chain_t* test_chain = ngx_alloc_chain_link(r->pool);
	ngx_buf_t* test_buf = (ngx_buf_t*)ngx_calloc_buf(r->pool);
	test_buf->pos = test_buf->start = ctx->pb_buf;
	test_buf->last = test_buf->end = (u_char*) (ctx->pb_buf +  ctx->pb_buf_pos);
	test_buf->temporary = 1;

	test_chain->buf = test_buf;
	test_chain->next = NULL;
	r->request_body->bufs = test_chain;
	r->headers_in.content_length_n = ctx->pb_buf_pos;

	// second , reset the requset header info , include content-length and x_bd_data_type
	char* str_content_length = (char*)ngx_pcalloc(r->pool, DEFAULT_STR_BUF_SIZE);
	int str_size = snprintf(str_content_length, DEFAULT_STR_BUF_SIZE, "%u", r->headers_in.content_length_n);

	r->headers_in.content_length->value.data = (u_char*) str_content_length;
	r->headers_in.content_length->value.len = str_size;

	ctx->data_type_header->value = g_msgpack_str;

	end_time = ngx_http_pbmsgpack_get_time_us();
	ctx->transfer_cost = end_time - start_time;

	ngx_http_finalize_request(r,NGX_DONE);

}

static char* ngx_http_pb_msgpack_transfer_merge_loc_conf(ngx_conf_t* cf, void* parent, void* child)
{
	ngx_http_pb_msgpack_transfer_loc_conf_t* prev = (ngx_http_pb_msgpack_transfer_loc_conf_t*) parent;
	ngx_http_pb_msgpack_transfer_loc_conf_t* conf = (ngx_http_pb_msgpack_transfer_loc_conf_t*) child;

	ngx_conf_merge_value(conf->pbmsgpack_transfer_flag, prev->pbmsgpack_transfer_flag, 0);
	ngx_conf_merge_value(conf->pbmsgpack_filter_flag, prev->pbmsgpack_filter_flag, 0);

	return NGX_CONF_OK;
}


static ngx_int_t ngx_http_pb_msgpack_header_filter(ngx_http_request_t* r )
{
	if(r->headers_out.status != NGX_HTTP_OK)
	{
		return ngx_http_next_header_filter(r);
	}

	ngx_http_pb_msgpack_transfer_loc_conf_t* loc_conf = NULL;
	loc_conf = (ngx_http_pb_msgpack_transfer_loc_conf_t*) ngx_http_get_module_loc_conf(r, ngx_http_pb_msgpack_transfer_module);
	// flag is unset/0 , skip deal it
	if(0 == loc_conf->pbmsgpack_filter_flag || NGX_CONF_UNSET == loc_conf->pbmsgpack_filter_flag )
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "output transfer flag is 0!");
		return ngx_http_next_header_filter(r);
	}

	if( r == r->main)
	{
		ngx_http_clear_content_length(r);
	}

	return ngx_http_next_header_filter(r);

}


static ngx_int_t ngx_http_pb_msgpack_body_filter(ngx_http_request_t* r , ngx_chain_t* in)
{
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "enter pb msgpack body filter!");
	ngx_http_pb_msgpack_transfer_loc_conf_t* loc_conf = NULL;
	ngx_http_pbmsgpack_transfer_ctx_t* ctx = NULL;
	int req_cmd = -1;
	int ret ;
	loc_conf = (ngx_http_pb_msgpack_transfer_loc_conf_t*) ngx_http_get_module_loc_conf(r, ngx_http_pb_msgpack_transfer_module);

	// flag is unset/0 , skip deal it
	if(0 == loc_conf->pbmsgpack_filter_flag || NGX_CONF_UNSET == loc_conf->pbmsgpack_filter_flag )
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "output transfer flag is 0!");
		return ngx_http_next_body_filter(r, in);
	}

	// response is failed, skip it
	if(r->headers_out.status != NGX_HTTP_OK)
	{
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "Bypassed");
		return ngx_http_next_body_filter(r, in);
    }


	ctx = (ngx_http_pbmsgpack_transfer_ctx_t*) ngx_http_get_module_ctx(r, ngx_http_pb_msgpack_transfer_module);
	if(NULL == ctx)
	{
		ctx = (ngx_http_pbmsgpack_transfer_ctx_t* )ngx_pcalloc(r->pool, sizeof(ngx_http_pbmsgpack_transfer_ctx_t) );
		if(NULL == ctx)
		{
			ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "ngx_pcalloc ngx_http_pbmsgpack_transfer_ctx_t failed");
			return ngx_http_next_body_filter(r,in);
		}

		ctx->msgpack_buf_size = ctx->pb_buf_size = g_pbmsgpack_buf_size;
		ctx->msgpack_buf = (u_char*) ngx_pcalloc(r->pool, ctx->msgpack_buf_size);
		ctx->msgpack_pos = 0;

		if(NULL == ctx->msgpack_buf)
		{
			ngx_log_error(NGX_LOG_ALERT, r->connection->log,0, "ngx_pcalloc ngx_http_pbmsgpack_transfer_ctx_t failed");
			return ngx_http_next_body_filter(r,in);
		}

		ngx_http_set_ctx(r, ctx, ngx_http_pb_msgpack_transfer_module);
	}
	else
	{
		if(NULL == ctx->msgpack_buf)
		{
			ctx->msgpack_buf_size = ctx->pb_buf_size = g_pbmsgpack_buf_size;
			ctx->msgpack_buf = (u_char*) ngx_pcalloc(r->pool, ctx->msgpack_buf_size);
			ctx->msgpack_pos = 0;
		}

		if(NULL == ctx->msgpack_buf)
		{
			ngx_log_error(NGX_LOG_ALERT, r->connection->log,0, "ngx_pcalloc ngx_http_pbmsgpack_transfer_ctx_t failed");
			return ngx_http_next_body_filter(r,in);
		}

	}

	if( 1 == ctx->has_filter_deal)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pbmsgpack filter transfer has already deal! skip it!");
		return ngx_http_next_body_filter(r,in);
	}

	if(NULL == ctx->sent_data_type_header)
	{
		// check whether the res data type is msgpack
		ngx_table_elt_t* res_data_type = ngx_http_pbmsgpack_get_res_header(r, & g_out_data_type_header);
		ctx->sent_data_type_header = res_data_type;

		if(NULL == res_data_type)
		{
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pbmsgpack sent_http_x_data_type is NULL, skip deal it!");
			ctx->has_filter_deal = 1;
			return ngx_http_next_body_filter(r, in);
		}
		else if( res_data_type->value.len != g_msgpack_nginx_str.len || 0 != memcmp(res_data_type->value.data, g_msgpack_nginx_str.data, g_msgpack_str.len))
		{
			ngx_log_debug1(NGX_LOG_DEBUG_HTTP,  r->connection->log, 0, "pbmsgpack sent_http_x_data_type is: %V, skip it", & res_data_type->value);
			ctx->has_filter_deal = 1;
			return ngx_http_next_body_filter(r, in);
		}

	}

	req_cmd = ngx_http_pbmsgpack_get_cmd(r);
	if( -1 == req_cmd)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0 ,"ngx_http_pbmsgpack_get_cmd failed, skip it");
		return ngx_http_next_body_filter(r, in);
	}


    ngx_chain_t *it;
    for(it = in; it; it = it->next) 
	{
        if(it->buf->last == it->buf->pos) 
		{
            ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack filter skip empty buf");
        } 
		else
	   	{
            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack output filter %db\n", (int)(it->buf->last - it->buf->pos));

			if(ctx->msgpack_pos + it->buf->last - it->buf->pos >= ctx->msgpack_buf_size)
			{
				ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "pbmsgpack output body size is too large!");
				return ngx_http_next_body_filter(r, in);
			}

			memcpy(ctx->msgpack_buf + ctx->msgpack_pos , it->buf->pos, it->buf->last - it->buf->pos);
			ctx->msgpack_pos += it->buf->last - it->buf->pos;
        }
        // mark as sent to client so nginx is happy 
        it->buf->pos = it->buf->last;
        
		if(it->buf->last_buf) 
		{

			unsigned long int start_time, end_time;
			start_time = ngx_http_pbmsgpack_get_time_us();

			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack output filter total size: %d ", ctx->msgpack_pos);
			//it->buf->last_buf = 0;


			ret = pkg_msgpack2pb(g_ctx, ctx->msgpack_buf, (int*) & ctx->msgpack_pos,  ctx->msgpack_buf_size, req_cmd, PKG_DOWNSTREAM);
			if(0 != ret)
			{
				ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "pkg_msgpack2pb failed! [ret:%d]", ret);
				// ctx->sent_data_type_header = g_protobuf_str;
				// we do not skip to the next body filter
				// because if failed, we will still send the msgpack info to client
				ctx->has_filter_deal = 1;
				return ngx_http_next_body_filter(r, in);
			}
			else
			{
				ctx->sent_data_type_header->value  = g_protobuf_nginx_str;
			}

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack output filter new size: %d ", ctx->msgpack_pos);

			// mark the filter deal flag
			ctx->has_filter_deal = 1;


/*
 *            ngx_chain_t * empty_chain = ngx_alloc_chain_link(r->pool);
 *            if(NULL == empty_chain)
 *            {
 *                return NGX_ERROR;
 *            }
 *            empty_chain->next = NULL;
 *
 *            ngx_buf_t* empty_buf = (ngx_buf_t*) ngx_calloc_buf(r->pool);
 *            empty_buf->memory = 1;
 *            empty_buf->last_buf = 1;
 *            empty_buf->pos = empty_buf->last = ctx->msgpack_buf;
 *            empty_chain->buf = empty_buf;
 */


//			ngx_chain_t* empty_chain = NULL;
			ngx_chain_t *rc = ngx_alloc_chain_link(r->pool);
			if(rc == NULL )
			{
				return NGX_ERROR;
			}
			//rc->next = it;
			rc->next = NULL;
			rc->buf = (ngx_buf_t*)ngx_calloc_buf(r->pool);
			if(rc->buf == NULL) 
			{
				return NGX_ERROR;
			}

			rc->buf->memory = 1;
			rc->buf->last_buf = 0;
			rc->buf->pos = ctx->msgpack_buf;
			rc->buf->last = ctx->msgpack_buf + ctx->msgpack_pos;

			end_time = ngx_http_pbmsgpack_get_time_us();
			ctx->filter_cost = end_time - start_time;

            // Link into the buffer chain
            return ngx_http_next_body_filter(r, rc);
        }
    }

    return ngx_http_next_body_filter(r, NULL);
}

void ngx_http_pb_msgpack_exit_worker(ngx_cycle_t* cycle)
{
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP,  cycle->log, 0, "ngx_http_pb_msgpack_exit_worker destory resource!");
	if(NULL != g_ctx)
	{
		pkg_ctx_free(g_ctx);
		g_ctx = NULL;
	}

	global_pkg_ctx_free();
	g_pkg_ctx = NULL;
}



static int get_multipart_form_header_info( ngx_http_request_t* r, ngx_table_elt_t* elt, char* bound)
{
	if(NULL == elt)
	{
		return 0;
	}

	if(elt->value.len > g_max_content_type_size || 0 == g_max_content_type_size)
	{
		return -1;
	}

	char temp_info[g_max_content_type_size];
	memcpy(temp_info, elt->value.data, elt->value.len);
	temp_info[elt->value.len] = '\0';


	if(NULL == strstr(temp_info,g_multipart_form))
	{
		return -1;
	}

	const char* boundy_pos = strstr(temp_info, g_bound_str);
	if(NULL == boundy_pos)
	{
		return -1;
	}

	strcpy(bound, boundy_pos + strlen(g_bound_str));

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "pb msgpack get bound :%s", bound);

	return 0;
}


int get_multipart_form_info(char* info, int info_size, const char* bound, const char* tag_name, int& start_pos, int& end_pos)
{
    // multipart form format definition
    
    static const char* const tag_seperator = "name=";
    int tag_sep_len = sizeof("name=") - 1;
    static const char* const line_seperator = "\r\n\r\n";
    int line_sep_len = sizeof("\r\n\r\n") - 1;
    int bound_len = strlen(bound);
    const char* search_start = info;
    const char* cur_res;
    const char* temp_tag_pos;
    const char* info_end = info + info_size;

    while(true)
    {
        // cjs_log("try deal...");
        //if(NULL == (cur_res = strstr(search_start, bound)))
        if(NULL == (cur_res = (const char*)memmem(search_start, info_end - search_start, bound, bound_len )))
        {
            // cjs_log("can't find bound");
            return -1;
        }

        if(NULL == (temp_tag_pos = (const char*)memmem(cur_res + bound_len, info_end - cur_res - bound_len, tag_seperator, tag_sep_len )))
        //if(NULL == (temp_tag_pos = strstr(cur_res + bound_len, tag_seperator)))
        {
            // cjs_log("can't find tag name");
            return -1;
        }


        if(0 != strncmp( temp_tag_pos + tag_sep_len, tag_name, strlen(tag_name)))
        {
            search_start = temp_tag_pos + tag_sep_len;
            continue;
        }
        
        //found the tag pos
        
        //const char* res_start_pos = strstr(temp_tag_pos + tag_sep_len, line_seperator);
        const char* res_start_pos = (const char*)memmem(temp_tag_pos + tag_sep_len, info_end - temp_tag_pos - tag_sep_len, line_seperator, line_sep_len );
        if(NULL == res_start_pos)
        {
            /// cjs_log("can't find line seperator in the tag");
            return -1;
        }

        //const char* res_end_pos = strstr(res_start_pos + line_sep_len, bound);
        const char* res_end_pos = (const char*)memmem(res_start_pos + line_sep_len, info_end - res_start_pos - line_sep_len, bound, bound_len );
        if(NULL == res_end_pos)
        {
            return -1;
        }

        start_pos = int(res_start_pos + line_sep_len - info);
        end_pos = int(res_end_pos - info) - line_sep_len;
        return 0;
    }

    return -1;
}


static ngx_int_t ngx_http_pbmsgpack_add_variables(ngx_conf_t *cf)
{
    ngx_http_variable_t  *var, *v;

    for (v = ngx_http_pbmsgpack_vars; v->name.len; v++) {
        var = ngx_http_add_variable(cf, &v->name, v->flags);
        if (var == NULL) {
            return NGX_ERROR;
        }

        var->get_handler = v->get_handler;
        var->data = v->data;
    }

    return NGX_OK;
}

ngx_int_t ngx_http_pbmsgpack_var(ngx_http_request_t *r,ngx_http_variable_value_t *v, uintptr_t data)
{
	ngx_http_pbmsgpack_transfer_ctx_t* ctx = NULL;
	ctx = (ngx_http_pbmsgpack_transfer_ctx_t*) ngx_http_get_module_ctx(r, ngx_http_pb_msgpack_transfer_module);
	if(NULL == ctx)
	{
		v->not_found = 1;
		return NGX_OK;
	}

	v->valid = 1;
	v->no_cacheable = 0;
	v->not_found = 0;

    v->data = (u_char*)ngx_pnalloc(r->connection->pool, NGX_SIZE_T_LEN);

    if (v->data == NULL) {
        return NGX_ERROR;
    }

	switch(data)
	{
		case PB_VAR_TYPE_TRANSFER_COST:
			v->len = ngx_sprintf(v->data, "%uz", ctx->transfer_cost) - v->data;
			break;

		case PB_VAR_TYPE_FILTER_COST:
			v->len = ngx_sprintf(v->data, "%uz", ctx->filter_cost) - v->data;
			break;

		case PB_VAR_TYPE_TRANSFER_SIZE:
			v->len = ngx_sprintf(v->data, "%uz", ctx->pb_buf_pos) - v->data;
			break;

		case PB_VAR_TYPE_FILTER_SIZE:
			v->len = ngx_sprintf(v->data, "%uz", ctx->msgpack_pos) - v->data;
			break;

		default:
			return NGX_ERROR;
	}

    return NGX_OK;
}

static unsigned long int ngx_http_pbmsgpack_get_time_us()
{
    const int S_TO_US = 1000000;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long int t = tv.tv_sec;
    t = t * S_TO_US + tv.tv_usec;

	return t;
}

