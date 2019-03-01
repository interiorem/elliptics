#ifndef ELLIPTICS_N2_PROTOCOL_H
#define ELLIPTICS_N2_PROTOCOL_H

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct n2_message;

struct dnet_cmd *n2_access_cmd(struct n2_message *);
void n2_message_free(struct n2_message *n2_msg);

#ifdef __cplusplus
}
#endif

#endif // ELLIPTICS_N2_PROTOCOL_H
