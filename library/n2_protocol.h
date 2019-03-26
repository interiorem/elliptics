#ifndef ELLIPTICS_N2_PROTOCOL_H
#define ELLIPTICS_N2_PROTOCOL_H

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_node;
struct n2_request_info;
struct n2_response_info;
struct n2_message;

struct dnet_cmd *n2_request_info_access_cmd(struct n2_request_info *req_info);
struct dnet_cmd *n2_response_info_access_cmd(struct n2_response_info *resp_info);

void n2_request_info_free(struct n2_request_info *req_info);
void n2_response_info_free(struct n2_response_info *resp_info);

#ifdef __cplusplus
}
#endif

#endif // ELLIPTICS_N2_PROTOCOL_H
