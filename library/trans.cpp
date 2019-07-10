#include "elliptics.h"
#include "elliptics/packet.h"
#include "elliptics/interface.h"
#include "library/logger.hpp"
#include "library/n2_protocol.hpp"

#include <blackhole/attribute.hpp>

int n2_trans_send(dnet_trans *t, n2_request_info *request_info); // Implemented in net.cpp

namespace ioremap { namespace elliptics {

int n2_trans_send_fail(const n2_request_info &request_info, int err)
{
	return request_info.repliers.on_last_error(err);
}

int n2_trans_alloc_send_state(dnet_session *s, dnet_net_state *st, n2_request_info &request_info)
{
	// TODO(sabramkin): share the logic with n2_trans_forward (a lot of common actions is done)

	dnet_node *n = st->n;
	dnet_cmd *cmd = &request_info.request.cmd;

	std::unique_ptr<dnet_trans, void (*)(dnet_trans *)>
	        t(dnet_trans_alloc(n, 0), &dnet_trans_put);
	if (!t) {
		return n2_trans_send_fail(request_info, -ENOMEM);
	}

	t->complete = nullptr;
	t->priv = nullptr;

	const timespec *wait_ts = dnet_session_get_timeout(s);
	t->wait_ts = *wait_ts;
	request_info.request.deadline = {static_cast<uint64_t>(wait_ts->tv_sec),
	                                 static_cast<uint64_t>(wait_ts->tv_nsec)};
	cmd->flags |= dnet_session_get_cflags(s);
	cmd->trace_id = dnet_session_get_trace_id(s);
	if (cmd->flags & DNET_FLAGS_DIRECT_BACKEND)
		cmd->backend_id = dnet_session_get_direct_backend(s);

	t->repliers = new n2_repliers; // Will be filled at native_protocol::send_request

	t->command = cmd->cmd;
	cmd->trans = t->rcv_trans = t->trans = atomic_inc(&n->trans);
	memcpy(&t->cmd, cmd, sizeof(struct dnet_cmd));

	t->st = dnet_state_get(st);

	DNET_LOG_INFO(n, "%s: %s: created %s",
	              dnet_dump_id(&cmd->id),
	              dnet_cmd_string(cmd->cmd),
	              dnet_print_trans(t.get()));

	// TODO(sabramkin): unique_ptr on trans?
	int err = n2_trans_send(t.get(), &request_info);
	if (err)
		return n2_trans_send_fail(request_info, err);

	t.release();
	return 0;
}

int n2_trans_alloc_send(dnet_session *s, n2_request_info &&request_info, dnet_addr &addr_out) {
	dnet_node *n = s->node;
	dnet_cmd *cmd = &request_info.request.cmd;
	dnet_net_state *st = nullptr;
	int err = 0;

	if (dnet_session_get_cflags(s) & DNET_FLAGS_DIRECT) {
		st = dnet_state_search_by_addr(n, &s->direct_addr);
	} else if (dnet_session_get_cflags(s) & DNET_FLAGS_FORWARD) {
		st = dnet_state_search_by_addr(n, &s->forward_addr);
	}else {
		st = dnet_state_get_first(n, &cmd->id);
	}

	if (!st) {
		DNET_LOG_ERROR(n, "%s: direct: %d, direct-addr: %s, forward: %d: trans_send: could not find network state for address",
		               dnet_dump_id(&cmd->id),
		               !!(dnet_session_get_cflags(s) & DNET_FLAGS_DIRECT), dnet_addr_string(&s->direct_addr),
		               !!(dnet_session_get_cflags(s) & DNET_FLAGS_FORWARD));
		err = n2_trans_send_fail(request_info, -ENXIO);
	} else {
		addr_out = st->addr;
		err = n2_trans_alloc_send_state(s, st, request_info);
		dnet_state_put(st);
	}

	return err;
}

}} // ioremap::elliptics
