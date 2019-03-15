#pragma once

struct dnet_node;
struct dnet_grpc_client_config;
struct dnet_grpc_server_config;

#ifdef __cplusplus
extern "C" {
#endif

int dnet_grpc_io_client_start(struct dnet_node *node, struct dnet_grpc_client_config *config);
int dnet_grpc_io_server_start(struct dnet_node *node, struct dnet_grpc_server_config *config);

void dnet_grpc_io_stop(struct dnet_node *node);

#ifdef __cplusplus
}
#endif
