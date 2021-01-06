import pika
import pika.credentials
import ssl
import os
import logging

WAGGLE_NODE_ID = os.environ.get('WAGGLE_NODE_ID', '0000000000000000')

def create_connection_parameters(host, port, username=None, password=None, cacertfile=None, certfile=None, keyfile=None):
    if username is not None:
        credentials = pika.PlainCredentials(username, password)
    else:
        credentials = pika.credentials.ExternalCredentials()

    if cacertfile is not None:
        context = ssl.create_default_context(cafile=cacertfile)
        # HACK this allows the host and baked in host to be configured independently
        context.check_hostname = False
        if certfile is not None:
            context.load_cert_chain(certfile, keyfile)
        ssl_options = pika.SSLOptions(context, host)
    else:
        ssl_options = None

    return pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        ssl_options=ssl_options,
        retry_delay=60,
        socket_timeout=10.0,
    )

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S")
    # pika logging is too verbose, so we turn it down.
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    src_params = create_connection_parameters(
        host=os.environ["SRC_HOST"],
        port=int(os.environ["SRC_PORT"]),
        username=os.environ.get("SRC_USERNAME"),
        password=os.environ.get("SRC_PASSWORD"),
        cacertfile=os.environ.get("SRC_CACERTFILE"),
        certfile=os.environ.get("SRC_CERTFILE"),
        keyfile=os.environ.get("SRC_KEYFILE"),
    )

    src_queue = os.environ["SRC_QUEUE"]

    dest_params = create_connection_parameters(
        host=os.environ["DEST_HOST"],
        port=int(os.environ["DEST_PORT"]),
        username=os.environ.get("DEST_USERNAME"),
        password=os.environ.get("DEST_PASSWORD"),
        cacertfile=os.environ.get("DEST_CACERTFILE"),
        certfile=os.environ.get("DEST_CERTFILE"),
        keyfile=os.environ.get("DEST_KEYFILE"),
    )

    dest_exchange = os.environ["DEST_EXCHANGE"]

    logging.info("connecting to dest %s:%d", dest_params.host, dest_params.port)
    dest_connection = pika.BlockingConnection(dest_params)
    dest_channel = dest_connection.channel()

    logging.info("connecting to src %s:%d", src_params.host, src_params.port)
    src_connection = pika.BlockingConnection(src_params)
    src_channel = src_connection.channel()

    user_id = f'node-{WAGGLE_NODE_ID}'

    def on_message(ch, method, properties, body):
        logging.info("shoveling message %s %s", properties.reply_to, method.routing_key)
        # TODO get user ID from credentials.
        properties.user_id = user_id
        dest_channel.basic_publish(dest_exchange, method.routing_key, body, properties=properties)
        src_channel.basic_ack(method.delivery_tag)
        logging.info("shoveled message")

    logging.info("starting message handler")
    src_channel.basic_consume(src_queue, on_message)
    src_channel.start_consuming()

if __name__ == '__main__':
    main()
