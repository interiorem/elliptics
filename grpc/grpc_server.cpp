#include "requests_common.hpp"

#include "read_request.hpp"
#include "write_request.hpp"

namespace fb = flatbuffers;

namespace ioremap { namespace elliptics { namespace grpc_dnet {

struct server {
public:
	server(dnet_node *node)
	: node_(node)
	{}

	~server() {
		server_->Shutdown();
		cq_->Shutdown();
		thread_->join();
	}

	void run() {
		std::string server_address("0.0.0.0:2025");

		grpc::ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(&service_);

		cq_ = builder.AddCompletionQueue();

		server_ = builder.BuildAndStart();

		DNET_LOG_INFO(node_, "GRPC: server listening on {}", server_address);

		thread_ = std::make_unique<std::thread>(std::bind(&server::completion_thread, this));
	}

	void completion_thread() {
		service_provider_info provider;
		provider.node = node_;
		provider.service = &service_;
		provider.cq = cq_.get();

		new read_request_manager(provider,
		                         [](std::unique_ptr<n2::read_request>){ /*TODO: implement*/ });
		new write_request_manager(provider,
		                          [](std::unique_ptr<n2::write_request>){ /*TODO: implement*/ });

		void *tag;
		bool ok;
		while (cq_->Next(&tag, &ok)) {
			static_cast<request_manager_base*>(tag)->process_completion(ok);
		}
	}

private:
	dnet_node *node_;
	std::unique_ptr<grpc::ServerCompletionQueue> cq_;
	fb_grpc_dnet::Elliptics::AsyncService service_;
	std::unique_ptr<grpc::Server> server_;
	std::unique_ptr<std::thread> thread_;
};

}}} // namespace ioremap::elliptics::grpc_dnet

// Out of namespace: C compatible API

struct dnet_grpc_server {
	ioremap::elliptics::grpc_dnet::server server;

	dnet_grpc_server(struct dnet_node *n)
	: server(n)
	{}
};

void dnet_start_grpc_server(struct dnet_node *n) {
	n->io->grpc = new dnet_grpc_server(n);
	n->io->grpc->server.run();
}

void dnet_stop_grpc_server(struct dnet_node *n) {
	delete std::exchange(n->io->grpc, nullptr);
}
