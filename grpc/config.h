#pragma once

struct dnet_grpc_client_config {
	unsigned thread_num;
};

#ifdef __cplusplus

#include <memory>
#include <string>

struct dnet_grpc_server_config {
	dnet_grpc_server_config();

	dnet_grpc_client_config client;
	std::string address;
};

namespace kora {

class config_t;

} // namespace kora

namespace ioremap { namespace grpc {

std::unique_ptr<dnet_grpc_server_config> parse_server_config(const kora::config_t &grpc);

}} // namespace ioremap::grpc

#endif
