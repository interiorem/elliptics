#include "bulk_remove_handler.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <blackhole/attribute.hpp>

#include "bindings/cpp/callback_p.h"
#include "bindings/cpp/node_p.hpp"
#include "bindings/cpp/session_internals.hpp"
#include "bindings/cpp/timer.hpp"

#include "library/access_context.h"
#include "library/elliptics.h"
#include "library/common.hpp"

#include "bindings/cpp/functional_p.h"


namespace ioremap { namespace elliptics { namespace newapi {

void single_bulk_remove_handler::start(const transport_control &control, const dnet_bulk_remove_request &request)
{
	DNET_LOG_NOTICE(m_log, "{}: started: address: {}, num_keys: {}",
		dnet_cmd_string(control.get_native().cmd), dnet_addr_string(&m_address),
		request.keys.size());

	auto rr = async_result_cast<remove_result_entry>(m_session, send_to_single_state(m_session, control));
	m_handler.set_total(rr.total());

	m_keys.assign(request.keys.begin(), request.keys.end());
	std::sort(m_keys.begin(), m_keys.end());
	m_key_responses.resize(m_keys.size(), false);

	rr.connect(
		std::bind(&single_bulk_remove_handler::process, shared_from_this(), std::placeholders::_1),
		std::bind(&single_bulk_remove_handler::complete, shared_from_this(), std::placeholders::_1)
	);
}

void single_bulk_remove_handler::process(const remove_result_entry &entry) {
	auto cmd = entry.command();

	if (!entry.is_valid()) {
		DNET_LOG_ERROR(m_log, "{}: {}: process: invalid response, status: {}",
			dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), cmd->status);
		return;
	}
	// mark responsed key
	bool found = false;
	for (auto it = std::lower_bound(m_keys.begin(), m_keys.end(), cmd->id); it != m_keys.end(); ++it) {
		if (dnet_id_cmp(&cmd->id, &*it) != 0)
			break;

		const auto index = std::distance(m_keys.begin(), it);

		if (m_key_responses[index])
			continue;

		m_handler.process(entry);

		m_key_responses[index] = true;
		found = true;
		break;
	}

	if (!found) {
		DNET_LOG_ERROR(m_log, "{}: {}: process: unknown key, status: {}",
			dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), cmd->status);
	}	
}

void single_bulk_remove_handler::complete(const error_info &error) {
	// process all non-responsed keys:
	dnet_cmd cmd;
	memset(&cmd, 0, sizeof(cmd));
	cmd.status = error ? error.code() : 0; // @AU 
	cmd.cmd = DNET_CMD_BULK_REMOVE_NEW;
	cmd.trace_id = m_session.get_trace_id();
	cmd.flags = DNET_FLAGS_REPLY | DNET_FLAGS_MORE |
		(m_session.get_trace_bit() ? DNET_FLAGS_TRACE_BIT : 0);

	for (size_t i = 0; i < m_keys.size(); ++i) {
		if (m_key_responses[i])
			continue;

		cmd.id = m_keys[i];
		auto result_data = std::make_shared<ioremap::elliptics::callback_result_data>(&m_address, &cmd);
		result_data->error = error ? error :
			create_error(0, "send_bulk_remove: remove failed for key: %s",
				dnet_dump_id(&m_keys[i]));
		ioremap::elliptics::callback_result_entry entry(result_data);
		m_handler.process(callback_cast<read_result_entry>(entry));
	}

	// finish
	m_handler.complete(error);
	DNET_LOG_NOTICE(m_log, "{}: finished: address: {}",
		dnet_cmd_string(DNET_CMD_BULK_REMOVE_NEW), dnet_addr_string(&m_address));
}


void bulk_remove_handler::start()
{
	DNET_LOG_INFO(m_log, "{}: started: keys: {}",
		dnet_cmd_string(DNET_CMD_BULK_REMOVE_NEW), m_keys.size());

	m_context.reset(new dnet_access_context(m_session.get_native_node()));
	if (m_context) {
		m_context->add({ {"cmd", std::string(dnet_cmd_string(DNET_CMD_BULK_REMOVE_NEW))},
				{"access", "client"},
				{"ioflags", std::string(dnet_flags_dump_ioflags(m_session.get_ioflags()))},
				{"cflags", std::string(dnet_flags_dump_cflags(m_session.get_cflags()))},
				{"keys", m_keys.size()},
				{"trace_id", to_hex_string(m_session.get_trace_id())},
			});
	}
	if (m_keys.empty()) {
		m_handler.complete(create_error(-ENXIO, "send_bulk_remove: keys list is empty"));
		return;
	}

	// group keys
	std::map<dnet_addr, std::vector<dnet_id>, dnet_addr_comparator> remote_ids; // node_address -> [list of keys]
	const bool has_direct_address = !!(m_session.get_cflags() & (DNET_FLAGS_DIRECT | DNET_FLAGS_DIRECT_BACKEND));
	std::vector<std::pair<dnet_id, int> > failed_ids;
	if (!has_direct_address) {
		m_session.split_keys_to_nodes(m_keys, remote_ids, failed_ids);
	}
	else {
		const auto address = m_session.get_direct_address();
		remote_ids.emplace(address.to_raw(), m_keys);
	}
	
	std::vector<async_remove_result> results;
	results.reserve(remote_ids.size());

	for (auto &pair : remote_ids) {
		const dnet_addr &address = pair.first;
		std::vector<dnet_id> &ids = pair.second;
		const dnet_bulk_remove_request request{ std::move(ids) };
		const auto packet = serialize(request);

		transport_control control;
		control.set_command(DNET_CMD_BULK_REMOVE_NEW);
		control.set_cflags(m_session.get_cflags() | DNET_FLAGS_NEED_ACK | DNET_FLAGS_NOLOCK);
		control.set_data(packet.data(), packet.size());

		auto session = m_session.clean_clone();
		if (!has_direct_address)
			session.set_direct_id(address);

		results.emplace_back(session); 
		auto handler = std::make_shared<single_bulk_remove_handler>(results.back(), session, address);
		handler->start(control, request);
	}

	auto rr = aggregated(m_session, results);
	m_handler.set_total(rr.total());

	rr.connect(
		std::bind(&bulk_remove_handler::process, shared_from_this(), std::placeholders::_1),
		std::bind(&bulk_remove_handler::complete, shared_from_this(), std::placeholders::_1)
	);

}
void bulk_remove_handler::process(const remove_result_entry &entry) {
	m_handler.process(entry);

	const auto *cmd = entry.command();
	m_transes.emplace(cmd->trans);
	auto it = m_statuses.emplace(entry.status(), 1);
	if (!it.second)
		++it.first->second;
}
void bulk_remove_handler::complete(const error_info &error) {
	m_handler.complete(error);

	if (m_context) {
		m_context->add({ {"transes", [&] {
					std::ostringstream result;
					result << m_transes;
					return std::move(result.str());
				}()},
				{"statuses", [&] {
					std::ostringstream result;
					result << m_statuses;
					return std::move(result.str());
				}()},
			});
		m_context.reset(); // destroy context to print access log
	}
}

} } } // namespace ioremap::elliptics::newapi