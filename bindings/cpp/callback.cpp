/*
 * 2008+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
 * 2012+ Copyright (c) Ruslan Nigmatullin <euroelessar@yandex.ru>
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 */

#include <blackhole/attribute.hpp>

#include "callback_p.h"

#include <blackhole/wrapper.hpp>

#include "library/elliptics.h"
#include "library/logger.hpp"

namespace ioremap { namespace elliptics {

namespace detail {

class basic_handler
{
public:
	static int handler(dnet_addr *addr, dnet_cmd *cmd, void *priv)
	{
		basic_handler *that = reinterpret_cast<basic_handler *>(priv);
		if (that->handle(addr, cmd)) {
			delete that;
		}

		return 0;
	}

	// Used by old (protocol-dependent) mechanic, must be removed after refactoring
	basic_handler(std::unique_ptr<dnet_logger> logger, async_generic_result &result) :
		m_logger(std::move(logger)),
		m_handler(result), m_completed(0), m_total(0)
	{
		memset(&m_addr, 0, sizeof(dnet_addr));
		memset(&m_cmd, 0, sizeof(dnet_cmd));
	}

	basic_handler(const dnet_cmd &cmd, std::unique_ptr<dnet_logger> logger, async_generic_result &result) :
		m_cmd(cmd),
		m_logger(std::move(logger)),
		m_handler(result), m_completed(0), m_total(0)
	{
		memset(&m_addr, 0, sizeof(dnet_addr));
	}

	void log_reply_info(dnet_addr *addr, dnet_cmd *cmd)
	{
		DNET_LOG(m_logger, cmd->status ? DNET_LOG_ERROR : DNET_LOG_NOTICE, "{}: {}: handled reply from: {}, "
		                                                                   "trans: {}, cflags: {}, status: {}, "
		                                                                   "size: {}, client: {}, last: {}",
		         dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), addr ? dnet_addr_string(addr) : "<unknown>",
		         cmd->trans, dnet_flags_dump_cflags(cmd->flags), int(cmd->status), cmd->size,
		         !(cmd->flags & DNET_FLAGS_REPLY), !(cmd->flags & DNET_FLAGS_MORE));
	}

	// Used by old (protocol-dependent) mechanic, must be removed after refactoring
	bool handle(dnet_addr *addr, dnet_cmd *cmd)
	{
		if (is_trans_destroyed(cmd)) {
			return increment_completed();
		}

		log_reply_info(addr, cmd);

		auto data = std::make_shared<callback_result_data>(addr, cmd);

		if (cmd->status)
			data->error = create_error(*cmd);

		callback_result_entry entry(data);

		m_handler.process(entry);

		return false;
	}

	int on_reply(const std::shared_ptr<n2_body> &result, bool is_last)
	{
		// TODO(sabramkin): Output only protocol-independent known info (currently old-mechanic logging used)
		log_reply_info(&m_addr, &m_cmd);

		auto data = std::make_shared<n2_callback_result_data>(m_addr, m_cmd, result, 0, is_last);
		callback_result_entry entry(data);
		m_handler.process(entry);

		increment_completed(); // TODO(sabramkin): correctly process trans destroying
		return 0;
	}

	int on_reply_error(int err, bool is_last)
	{
		// TODO(sabramkin): Output only protocol-independent known info (currently old-mechanic logging used)
		log_reply_info(&m_addr, &m_cmd);

		auto data = std::make_shared<n2_callback_result_data>(m_addr, m_cmd, nullptr, err, is_last);
		data->error = create_error(err, "n2 lookup_new error"); // TODO(sabramkin): rework error
		callback_result_entry entry(data);
		m_handler.process(entry);

		increment_completed(); // TODO(sabramkin): correctly process trans destroying
		return 0;
	}

	// how many independent transactions share this handler plus call below
	// call below and corresponding +1 is needed, since transactions can be completed
	// before send_impl() calls this method to setup this 'reference counter'
	bool set_total(size_t total)
	{
		m_handler.set_total(total);
		m_total = total + 1;
		return increment_completed();
	}

private:
	bool increment_completed()
	{
		if (++m_completed == m_total) {
			m_handler.complete(error_info());
			return true;
		}

		return false;
	}

public:
	dnet_addr m_addr;

private:
	dnet_cmd m_cmd;
	std::unique_ptr<dnet_logger> m_logger;
	async_result_handler<callback_result_entry> m_handler;
	std::atomic_size_t m_completed;
	std::atomic_size_t m_total;
};

} // namespace detail

template <typename Method, typename T>
async_generic_result send_impl(session &sess, T &control, Method method)
{
	async_generic_result result(sess);

	detail::basic_handler *handler = new detail::basic_handler(sess.get_logger(), result);
	control.complete = detail::basic_handler::handler;
	control.priv = handler;

	const size_t count = method(sess, control);

	if (handler->set_total(count))
		delete handler;

	return result;
}

static size_t send_to_single_state_impl(session &sess, dnet_trans_control &ctl)
{
	dnet_trans_alloc_send(sess.get_native(), &ctl);
	return 1;
}

// Send request to specifically set state by id
async_generic_result send_to_single_state(session &sess, const transport_control &control)
{
	dnet_trans_control writable_copy = control.get_native();
	return send_impl(sess, writable_copy, send_to_single_state_impl);
}

static size_t send_to_single_state_io_impl(session &sess, dnet_io_control &ctl)
{
	dnet_io_trans_alloc_send(sess.get_native(), &ctl);
	return 1;
}

async_generic_result send_to_single_state(session &sess, dnet_io_control &control)
{
	return send_impl(sess, control, send_to_single_state_io_impl);
}

template <typename Method>
async_generic_result n2_send_impl(session &sess, const n2_request &request, Method method)
{
	async_generic_result result(sess);

	auto handler = std::make_shared<detail::basic_handler>(request.cmd, sess.get_logger(), result);

	auto calls_counter = std::make_shared<std::atomic<bool>>(false);
	auto test_and_set_reply_has_sent = [calls_counter](bool last) {
		if (last) {
			return calls_counter->exchange(true);
		} else {
			return bool(*calls_counter);
		}
	};

	n2_request_info request_info{ request, n2_repliers() };

	request_info.repliers.on_reply =
		[handler, test_and_set_reply_has_sent](const std::shared_ptr<n2_body> &result, bool last){
			if (test_and_set_reply_has_sent(last)) {
				return -EALREADY;
			}

			return handler->on_reply(result, last);
		};
	request_info.repliers.on_reply_error =
		[handler, test_and_set_reply_has_sent](int err, bool last){
			if (test_and_set_reply_has_sent(last)) {
				return -EALREADY;
			}

			return handler->on_reply_error(err, last);
		};

	const size_t count = method(sess, std::move(request_info), handler->m_addr);
	handler->set_total(count);
	return result;
}

int n2_trans_alloc_send(dnet_session *s, n2_request_info &&request_info, dnet_addr &addr_out); // implemented in trans.cpp

static size_t n2_send_to_single_state_impl(session &sess, n2_request_info &&request_info, dnet_addr &addr_out)
{
	n2_trans_alloc_send(sess.get_native(), std::move(request_info), addr_out);
	return 1;
}

async_generic_result n2_send_to_single_state(session &sess, const n2_request &request)
{
	return n2_send_impl(sess, request, n2_send_to_single_state_impl);
}

static size_t send_to_each_backend_impl(session &sess, dnet_trans_control &ctl)
{
	return dnet_request_cmd(sess.get_native(), &ctl);
}

// Send request to each backend
async_generic_result send_to_each_backend(session &sess, const transport_control &control)
{
	dnet_trans_control writable_copy = control.get_native();
	return send_impl(sess, writable_copy, send_to_each_backend_impl);
}

static size_t send_to_each_node_impl(session &sess, dnet_trans_control &ctl)
{
	dnet_node *node = sess.get_native_node();
	dnet_session *native_sess = sess.get_native();
	dnet_net_state *st;

	ctl.cflags |= DNET_FLAGS_DIRECT;
	size_t count = 0;

	pthread_mutex_lock(&node->state_lock);
	rb_for_each_entry(st, &node->dht_state_root, node_entry) {
		if (st == node->st)
			continue;

		dnet_trans_alloc_send_state(native_sess, st, &ctl);
		++count;
	}
	pthread_mutex_unlock(&node->state_lock);

	return count;
}

async_generic_result send_to_each_node(session &sess, const transport_control &control)
{
	dnet_trans_control writable_copy = control.get_native();
	return send_impl(sess, writable_copy, send_to_each_node_impl);
}

static size_t send_to_groups_impl(session &sess, dnet_trans_control &ctl)
{
	dnet_session *native = sess.get_native();
	size_t counter = 0;

	for (int i = 0; i < native->group_num; ++i) {
		ctl.id.group_id = native->groups[i];
		dnet_trans_alloc_send(native, &ctl);
		++counter;
	}

	return counter;
}

// Send request to one state at each session's group
async_generic_result send_to_groups(session &sess, const transport_control &control)
{
	dnet_trans_control writable_copy = control.get_native();
	return send_impl(sess, writable_copy, send_to_groups_impl);
}

static size_t send_to_groups_io_impl(session &sess, dnet_io_control &ctl)
{
	return dnet_trans_create_send_all(sess.get_native(), &ctl);
}

async_generic_result send_to_groups(session &sess, dnet_io_control &control)
{
	return send_impl(sess, control, send_to_groups_io_impl);
}

} } // namespace ioremap::elliptics
