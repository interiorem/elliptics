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

#ifndef CALLBACK_P_H
#define CALLBACK_P_H

#include "elliptics/cppdef.h"

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "elliptics/async_result_cast.hpp"
#include "library/n2_protocol.hpp"

/*
 * This macroses should be used surrounding all entry::methods which work directly
 * with m_data or data() to ensure that meaningful exceptions are thrown
 */
#define DNET_DATA_BEGIN_2() try { \
	do {} while (false)

#define DNET_DATA_END_2(SIZE) \
	} catch (not_found_error &) { \
		if (!is_valid()) { \
			throw_error(-ENOENT, "entry::%s(): entry is null", __FUNCTION__); \
		} else {\
			dnet_cmd *cmd = command(); \
			throw_error(-ENOENT, cmd->id, "entry::%s(): data.size is too small, expected: %zu, actual: %zu, status: %d", \
				__FUNCTION__, size_t(SIZE), data.size(), cmd->status); \
		} \
		throw; \
	} \
	do {} while (false)

namespace ioremap { namespace elliptics {

class session_scope
{
	public:
		session_scope(session &sess) : m_sess(sess)
		{
			m_filter = m_sess.get_filter();
			m_checker = m_sess.get_checker();
			m_policy = m_sess.get_exceptions_policy();
			m_cflags = m_sess.get_cflags();
			m_ioflags = m_sess.get_ioflags();
		}

		~session_scope()
		{
			m_sess.set_filter(m_filter);
			m_sess.set_checker(m_checker);
			m_sess.set_exceptions_policy(m_policy);
			m_sess.set_cflags(m_cflags);
			m_sess.set_ioflags(m_ioflags);
		}

	private:
		session &m_sess;
		result_filter m_filter;
		result_checker m_checker;
		uint64_t m_cflags;
		uint32_t m_ioflags;
		uint32_t m_policy;
};

// TODO(sabramkin): This abstraction is temporary and used while refactoring in progress.
// TODO(sabramkin): After refactoring only n2_callback_result_data should stay, no base is needed.
class callback_result_data_base
{
	public:
		virtual dnet_addr *address() const = 0;
		virtual dnet_cmd *command() const = 0;
		virtual int status() const = 0;
		virtual bool is_valid() const = 0;
		virtual bool is_ack() const = 0;
		virtual bool is_final() const = 0;
		virtual bool is_client() const = 0;

		virtual ~callback_result_data_base() = default;

		error_info error;

		// Hint to determine derived without dynamic_cast. Reason: lookup_result_entry is used by two handlers:
		// session::lookup and session::write. The first one is converted to protocol-independent, but the
		// second one isn't.
		bool tmp_is_n2_protocol;
};

class callback_result_data : public callback_result_data_base
{
	public:
		callback_result_data()
		{
			tmp_is_n2_protocol = false;
		}

		callback_result_data(const dnet_addr *addr, const dnet_cmd *cmd)
		{
			tmp_is_n2_protocol = false;

			const size_t size = sizeof(dnet_addr) + sizeof(dnet_cmd) + cmd->size;
			data = data_pointer::allocate(size);
			if (addr)
				memcpy(data.data(), addr, sizeof(dnet_addr));
			else
				memset(data.data(), 0, sizeof(dnet_addr));
			memcpy(data.data<char>() + sizeof(dnet_addr), cmd, sizeof(dnet_cmd) + cmd->size);
		}

		dnet_addr *address() const override
		{
			DNET_DATA_BEGIN_2();
			return data
				.data<dnet_addr>();
			DNET_DATA_END_2(0);
		}

		dnet_cmd *command() const override
		{
			DNET_DATA_BEGIN_2();
			return data
				.skip<dnet_addr>()
				.data<dnet_cmd>();
			DNET_DATA_END_2(0);
		}

		int status() const override
		{
			return command()->status;
		}

		bool is_valid() const override
		{
			return !data.empty();
		}

		bool is_ack() const override
		{
			return status() == 0 && data.empty();
		}

		bool is_final() const override
		{
			return !(command()->flags & DNET_FLAGS_MORE);
		}

		bool is_client() const override
		{
			return !(command()->flags & DNET_FLAGS_REPLY);
		}

		data_pointer data;
};

class n2_callback_result_data : public callback_result_data_base
{
	public:
		n2_callback_result_data()
		: is_result_assigned(false)
		{
			tmp_is_n2_protocol = true;
		}

		n2_callback_result_data(const dnet_addr &addr_in, const dnet_cmd &cmd_in,
		                        const std::shared_ptr<n2_body> &result_body_in, int result_status_in,
		                        bool is_last_in)
		: addr(addr_in)
		, cmd(cmd_in)
		, is_result_assigned(true)
		, result_body(result_body_in)
		, result_status(result_status_in)
		, is_last(is_last_in)
		{
			tmp_is_n2_protocol = true;

			// TODO(sabramkin):
			// Here is emulated protocol logic for single-response commands. It is hardcode that we must
			// resolve when we introduce bulk commands. See also is_final() method. Note that protocol
			// mustn't provide its inner structures (such as dnet_cmd), so we must remove command() method
			// in the future, and must remove cmd member.
			cmd.flags = (cmd.flags & ~(DNET_FLAGS_NEED_ACK)) | DNET_FLAGS_REPLY;
			cmd.status = result_status_in;
		}

		dnet_addr *address() const override
		{
			return const_cast<dnet_addr *>(&addr);
		}

		dnet_cmd *command() const override
		{
			return const_cast<dnet_cmd *>(&cmd);
		}

		int status() const override
		{
			return result_status;
		}

		bool is_valid() const override
		{
			return is_result_assigned;
		}

		bool is_ack() const override
		{
			return result_status == 0 && !result_body;
		}

		bool is_final() const override
		{
			return is_last;
		}

		bool is_client() const override
		{
			return false;
		}

		dnet_addr addr;
		dnet_cmd cmd;

		// Either result or nonzero result_status must be set
		bool is_result_assigned;
		std::shared_ptr<n2_body> result_body;
		int result_status;
		bool is_last;
};

struct dnet_net_state_deleter
{
	void operator () (dnet_net_state *state) const
	{
		if (state)
			dnet_state_put(state);
	}
};

typedef std::unique_ptr<dnet_net_state, dnet_net_state_deleter> net_state_ptr;

// Send request to specific state
async_generic_result send_to_single_state(session &sess, const transport_control &control);
async_generic_result send_to_single_state(session &sess, dnet_io_control &control);
async_generic_result n2_send_to_single_state(session &sess, const n2_request &request);

// Send request to each backend
async_generic_result send_to_each_backend(session &sess, const transport_control &control);

// Send request to each node
async_generic_result send_to_each_node(session &sess, const transport_control &control);

// Send request to one state at each session's group
async_generic_result send_to_groups(session &sess, const transport_control &control);
async_generic_result send_to_groups(session &sess, dnet_io_control &control);

template <typename Handler, typename Entry>
class multigroup_handler : public std::enable_shared_from_this<multigroup_handler<Handler, Entry>>
{
public:
	typedef multigroup_handler<Handler, Entry> parent_type;

	multigroup_handler(const session &sess, const async_result<Entry> &result, std::vector<int> &&groups) :
		m_sess(sess.clean_clone()),
		m_handler(result),
		m_groups(std::move(groups)),
		m_group_index(0)
	{
		m_sess.set_checker(sess.get_checker());
	}

	void start()
	{
		if (m_groups.empty()) {
			m_handler.complete(error_info());
			return;
		}

		next_group();
	}

	void process(const Entry &entry)
	{
		process_entry(entry);

		m_handler.process(entry);
	}

	void complete(const error_info &error)
	{
		group_finished(error);
		++m_group_index;

		if (m_group_index < m_groups.size() && need_next_group(error)) {
			next_group();
		} else {
			m_handler.complete(error_info());
		}
	}

	void set_total(size_t total)
	{
		m_handler.set_total(total);
	}

protected:
	int current_group()
	{
		return m_groups[m_group_index];
	}

	void next_group()
	{
		using std::placeholders::_1;

		async_result_cast<Entry>(m_sess, send_to_next_group()).connect(
			std::bind(&multigroup_handler::process, this->shared_from_this(), _1),
			std::bind(&multigroup_handler::complete, this->shared_from_this(), _1)
		);
	}

	// Override this if you want to do something on each received packet
	virtual void process_entry(const Entry &entry)
	{
		(void) entry;
	}

	// Override this if you want to change the stop condition
	virtual bool need_next_group(const error_info &error)
	{
		return !!error;
	}

	// Override this if you want to do something before going to next group
	virtual void group_finished(const error_info &error)
	{
		(void) error;
	}

	// Override this to implement your send logic
	virtual async_generic_result send_to_next_group() = 0;

	session m_sess;
	async_result_handler<Entry> m_handler;
	const std::vector<int> m_groups;
	size_t m_group_index;
};

class net_state_id
{
public:
	net_state_id() : m_backend(-1)
	{
	}

	net_state_id(net_state_id &&other) : m_state(std::move(other.m_state)), m_backend(other.m_backend)
	{
	}

	net_state_id(dnet_node *node, const dnet_id *id) : m_backend(-1)
	{
		reset(node, id);
	}

	net_state_id &operator =(net_state_id &&other)
	{
		m_state = std::move(other.m_state);
		m_backend = other.m_backend;
		return *this;
	}

	void reset(dnet_node *node, const dnet_id *id)
	{
		m_backend = -1;
		m_state.reset(dnet_state_get_first_with_backend(node, id, &m_backend));
	}

	void reset()
	{
		m_state.reset();
		m_backend = -1;
	}

	dnet_net_state *operator ->() const
	{
		return m_state.get();
	}

	bool operator ==(const net_state_id &other)
	{
		return m_state == other.m_state && m_backend == other.m_backend;
	}

	bool operator !() const
	{
		return !m_state;
	}

	operator bool() const
	{
		return !!m_state;
	}

	dnet_net_state *state() const
	{
		return m_state.get();
	}

	int backend() const
	{
		return m_backend;
	}

private:
	net_state_ptr m_state;
	int m_backend;
};

} } // namespace ioremap::elliptics

#endif // CALLBACK_P_H
