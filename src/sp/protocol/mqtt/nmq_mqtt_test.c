#include <nuts.h>

#include "nng/protocol/mqtt/mqtt.h"
#include "nng/protocol/mqtt/nmq_mqtt.h"
#include "nng/supplemental/nanolib/conf.h"
#include "supplemental/mqtt/mqtt_msg.h"

static void
test_dropped_messages_option(void)
{
	conf      *config = nng_zalloc(sizeof(*config));
	nng_socket socket = { 0 };
	uint64_t   dropped = UINT64_MAX;
	uint64_t   sent = UINT64_MAX;
	bool       log_drops;

	NUTS_TRUE(config != NULL);
	conf_init(config);
	socket.data = config;
	NUTS_PASS(nng_nmq_tcp0_open(&socket));
	NUTS_PASS(nng_socket_get_uint64(
	    socket, NMQ_OPT_MQTT_MSGS_DROPPED, &dropped));
	NUTS_ASSERT(dropped == 0);
	NUTS_PASS(nng_socket_get_uint64(
	    socket, NMQ_OPT_MQTT_MSGS_SENT, &sent));
	NUTS_ASSERT(sent == 0);
	NUTS_FAIL(nng_socket_set_uint64(
	              socket, NMQ_OPT_MQTT_MSGS_DROPPED, 1),
	    NNG_EREADONLY);
	NUTS_PASS(nng_socket_get_bool(
	    socket, NMQ_OPT_MQTT_LOG_DROPS, &log_drops));
	NUTS_TRUE(log_drops);
	NUTS_PASS(nng_socket_set_bool(socket, NMQ_OPT_MQTT_LOG_DROPS, false));
	NUTS_PASS(nng_socket_get_bool(
	    socket, NMQ_OPT_MQTT_LOG_DROPS, &log_drops));
	NUTS_ASSERT(!log_drops);
	NUTS_PASS(nng_close(socket));
}

static void
count_completion(void *arg)
{
	int *completions = arg;

	(*completions)++;
}

static void
test_send_completion_and_broker_dispatch(void)
{
	conf      *config = nng_zalloc(sizeof(*config));
	nng_socket socket = { 0 };
	nng_ctx    ctx;
	nng_aio   *aio;
	nng_msg   *msg;
	uint32_t   missing_pipe = 42;
	uint64_t   dropped;
	int        completions = 0;

	NUTS_TRUE(config != NULL);
	conf_init(config);
	socket.data = config;
	NUTS_PASS(nng_nmq_tcp0_open(&socket));
	NUTS_PASS(nng_ctx_open(&ctx, socket));
	NUTS_PASS(nng_aio_alloc(&aio, count_completion, &completions));
	NUTS_PASS(nng_msg_alloc(&msg, 0));
	nng_msg_set_cmd_type(msg, CMD_PUBLISH);
	nng_aio_set_msg(aio, msg);
	nng_aio_set_prov_data(aio, &missing_pipe);
	nng_ctx_send(ctx, aio);
	nng_aio_wait(aio);
	NUTS_PASS(nng_aio_result(aio));
	NUTS_ASSERT(nng_aio_get_msg(aio) == NULL);
	NUTS_ASSERT(completions == 1);

	NUTS_PASS(nng_msg_alloc(&msg, 0));
	nng_msg_set_cmd_type(msg, CMD_PUBLISH);
	NUTS_PASS(nng_nmq_broker_send(ctx, missing_pipe, msg));
	NUTS_PASS(nng_socket_get_uint64(
	    socket, NMQ_OPT_MQTT_MSGS_DROPPED, &dropped));
	NUTS_ASSERT(dropped == 2);

	nng_aio_free(aio);
	NUTS_PASS(nng_ctx_close(ctx));
	NUTS_PASS(nng_close(socket));
}

NUTS_TESTS = {
	{ "nmq mqtt dropped messages option", test_dropped_messages_option },
	{ "nmq mqtt send completion and broker dispatch",
	    test_send_completion_and_broker_dispatch },
	{ NULL, NULL },
};
