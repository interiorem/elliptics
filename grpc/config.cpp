#include "config.h"

#include <kora/config.hpp>

dnet_grpc_server_config::dnet_grpc_server_config() {
	memset(&client, 0, sizeof(dnet_grpc_client_config));
}

namespace ioremap { namespace grpc {

std::unique_ptr<dnet_grpc_server_config> parse_server_config(const kora::config_t &grpc) {
	auto cfg = std::make_unique<dnet_grpc_server_config>();
	cfg->client.thread_num = grpc.at<unsigned>("thread_num", 0);
	cfg->address = grpc.at<std::string>("address");
	return cfg;
}

}} // namespace ioremap::grpc
