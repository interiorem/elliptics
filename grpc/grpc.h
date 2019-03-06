#pragma once

struct dnet_node;
struct dnet_grpc_io;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Init state for gRPC IO in the `node` object. State contains thread pool with `thread_num` threads for serving
 * client requests and server handlers. If `address` is specified, server will be initialized and will listen on
 * this address and port. Otherwise state will only allow to send client's requests.
 */
int dnet_grpc_io_start(struct dnet_node *node, const char *address, unsigned thread_num);

/*
 * Stop all gRPC work and destroy gRPC state in the `node`.
 */
void dnet_grpc_io_stop(struct dnet_node *node);

#ifdef __cplusplus
}
#endif
