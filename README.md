

## nginx pbmsgpack transfer module 说明 ##

author : chenjiansen@baidu.com  
date : 2014/4/16 18:00

---

### 概述 ###

ngx_pbmsgpack_transfer_module 是一个用于支持贴吧客户端长连接迁移项目的nginx扩展，其主要功能是做protobuf和msgpack的格式转换，具体是在请求的时候，将protobuf格式的post body里面的内容转成msgpack格式； 在请求输出的时候将msgpack格式转成protobuf格式。  

实现层面的话，主要是设置一个access phase 和 filter phase 的钩子，采用lcs提供的protobuf/msgpack 转换lib。  

---

### 指令 ###

1. __pbmsgpak_flag__ : [on | off]  
	__context__ : http  
	__default__ : off  
	__说明__：控制是否开启该模块  

2. __pbmsgpack\_buf\_size__ : buffer\_size  
	__context__ : http  
	__default__ : 16k  
	__说明__ :  设置转换时候申请的buffer 大小  

3. __pbmsgpack\_transfer\_flag__ : [ on | off]  
	__context__ : location  
	__default__ : off  
	__说明__ : 请求进来的时候是否做 protobuf 到 msgpack 的转换,  需要满足如下条件的时候才会转换:    
	1) 请求头部里面 x\_bd\_data\_type 为 msgpack  
	2) 请求参数里面含有cmd字段  
	3) 请求里面的post body必须存在，且要求 content_type 为 multi-part form 的格式  
	处理成功的时候模块会将 x\_bd\_data\_type 设置为 protobuf

4. __pbmsgpack\_filter\_flag__ : [on|off]  
	__context__ : location  
	__default__ : off  
	__说明__ : 请求出去的时候是否做 msgpack 到 protobuf 的转换，需要满足如下条件时候才转换:  
	1) 请求返回的x\_bd\_data\_type 必须为 : msgpack,nginx  
	2) 请求参数里面含有cmd字段  
	处理成功的时候会将返回的 x\_bd\_data\_type 设置为 protobuf,nginx  

---

### 变量  ###

1. _pbmsgpack\_transfer\_size_ : 转换之后的包体大小, 如果没有转换，则为0
2. _pbmsgpack\_transfer\_cost_ : 转换包体所用的时间耗时，单位是微秒
3. _pbmsgpack\_filter\_size_ : filter(msgpack2protobuf) 之后的包体大小
4. _pbmsgpack\_filter\_cost_ : filter(msgpack2protobuf)的耗时，单位是微秒


---

### 配置demo ###

	
	#http context  
	pbmsgpack_flag on;  
	pbmsgpack_conf_path "./conf/proto.conf";  
	pbmsgpack_buf_size 128k;  

	#location context  

	location = /cjs_test {

	pbmsgpack_transfer_flag on;
	pbmsgpack_filter_flag on;
	fastcgi_pass xxx;
	...
	}








