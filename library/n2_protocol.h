#ifndef ELLIPTICS_N2_PROTOCOL_H
#define ELLIPTICS_N2_PROTOCOL_H

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_node;
struct n2_call;
struct n2_message;

struct dnet_cmd *n2_message_access_cmd(struct n2_message *msg);

int n2_call_get_request(struct dnet_node *n, struct n2_call *call_data, struct n2_message **msg);
void n2_call_free(struct n2_call *call_data);

#ifdef __cplusplus
}
#endif

#endif // ELLIPTICS_N2_PROTOCOL_H
