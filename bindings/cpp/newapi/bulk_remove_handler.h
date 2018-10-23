#pragma once

#include "elliptics/newapi/session.hpp"
#include "elliptics/async_result_cast.hpp"
#include "library/protocol.hpp"

#include <unordered_map>

namespace ioremap { namespace elliptics { namespace newapi {

class single_bulk_remove_handler : public std::enable_shared_from_this<single_bulk_remove_handler> {
public:
	explicit single_bulk_remove_handler(const async_remove_result &result,
		const session &session,
		const dnet_addr &address)
		: m_address(address)
		, m_session(session.clean_clone())
		, m_handler(result)
		, m_log(session.get_logger())	{}

	void start(const transport_control &control, const dnet_bulk_remove_request &request);
private:
	void process(const remove_result_entry &entry);
	void complete(const error_info &error);
private:
	std::vector<dnet_id> m_keys; // stores original kes from request
	const dnet_addr m_address;
	session m_session;
	async_result_handler<remove_result_entry> m_handler;
	std::unique_ptr<dnet_logger> m_log;
	std::vector<bool> m_key_responses;

	std::unique_ptr<dnet_access_context> m_context;
};

class bulk_remove_handler : public std::enable_shared_from_this<bulk_remove_handler> {
public:
	explicit bulk_remove_handler(const async_remove_result &result,
		const session &session,
		const std::vector<dnet_id>& keys)
		: m_keys(keys)
		, m_session(session.clean_clone())
		, m_handler(result)
		, m_log(session.get_logger()) {}

	void start();
private:
	void process(const remove_result_entry &entry);
	void complete(const error_info &error);
private:
	const std::vector<dnet_id> m_keys;
	session m_session;
	async_result_handler<remove_result_entry> m_handler;
	std::unique_ptr<dnet_logger> m_log;

	std::unordered_set<uint64_t> m_transes;
	std::unordered_map<int, size_t> m_statuses;
	std::unique_ptr<dnet_access_context> m_context;
};

}
}
} // namespace ioremap::elliptics::newapi