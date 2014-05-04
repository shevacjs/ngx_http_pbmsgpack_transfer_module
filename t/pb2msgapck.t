# vi:filetype=

use lib 'lib';
use Test::Nginx::Socket;

repeat_each(1);

#plan tests => repeat_each() * (2 * blocks() + 1);
plan tests => repeat_each() * ( 2 * blocks());

#$Test::Nginx::LWP::LogLevel = 'debug';

run_tests();

__DATA__

=== TEST 1: sanity
--- http_config
	underscores_in_headers on;
	chunked_transfer_encoding off;
	pbmsgpack_flag on;
	pbmsgpack_conf_path "/home/users/chenjiansen/dev/request_body_transfer/nginx/modules/pbmsgpack-transfer-module/conf/proto.conf";
	pbmsgpack_buf_size 16k;

	log_format main '$http_x_bd_data_type $request_body $pbmsgpack_transfer_cost $status $sent_http_Transfer_Encoding';
	access_log /home/users/chenjiansen/dev/request_body_transfer/nginx/modules/pbmsgpack-transfer-module/t/servroot/logs/access.log main;
	
--- config
    location = /pb2msgpack {
		pbmsgpack_transfer_flag on;
#		echo "shevacjs";
		echo $request_body;
#		echo_request_body;
    }

--- more_headers
Content-type: multipart/form-data; boundary=--------7da3d81520810*
x_bd_data_type: protobuf


--- request eval
my $test = "shevacjs";
my $bindata;
open(FF, '</home/users/chenjiansen/dev/pbtest/163_cspv_body.txt') or die "no such file\n";
binmode(FF);
read(FF,$bindata,10240);
close(FF);
"POST /pb2msgpack?cmd=303001
$bindata"

--- response_body_like
.*imgType.*




