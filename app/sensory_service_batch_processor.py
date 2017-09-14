import lib.docker_config as config
from lib import filesystem_connecter
import logging
import json
import uuid
import pika
import time
import numpy


# Setup logging.
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO,
    filemode='w',
    filename="%s/sensory_service_batch_processor.log" % config.LOGS_DIR
)

logging.debug("creating a new fsc..")
fsc = filesystem_connecter.FileSystemConnector(config.DATA_DIRECTORY)

url = config.SENSORY_SERVICE_RABBITMQ_URL
parameters = pika.URLParameters(url)
connection = pika.BlockingConnection(parameters=parameters)

channel = connection.channel()

# channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue=config.SENSORY_SERVICE_RABBITMQ_BATCH_REQUEST_TASK_QUEUE, durable=True)


def process_batch_order(batch_order):
    order_metadata = json.loads(batch_order)
    logging.debug(order_metadata['sensory_batch_request_id'])
    logging.debug(order_metadata['noise'])
    logging.debug(order_metadata['batch_size'])

    sensory_batch_request_id = order_metadata['sensory_batch_request_id']
    noise = order_metadata['noise']
    batch_size = order_metadata['batch_size']

    image_data, image_labels = fsc.get_batch(batch_size, noise=noise)

    # iterate over the result
    outbound_connection = pika.BlockingConnection(parameters=parameters)
    uuid_channel = outbound_connection.channel()
    arguments = {'x-expires': 120 * 1000}  # 120 seconds
    uuid_channel.queue_declare(queue=sensory_batch_request_id, durable=False, auto_delete=True, arguments=arguments)

    uuid_channel.queue_bind(sensory_batch_request_id, config.SENSORY_SERVICE_RABBITMQ_EXCHANGE, routing_key=sensory_batch_request_id, arguments=None)

    for index in range(0, len(image_labels)):
        outbound_message = json.dumps({'label': numpy.array(image_labels[index]).tolist(), 'data': numpy.array(image_data[index]).tolist()})
        uuid_channel.basic_publish(exchange=config.SENSORY_SERVICE_RABBITMQ_EXCHANGE,
                                   routing_key=sensory_batch_request_id,
                                   body=outbound_message
                                   )
    outbound_connection.close()



print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    logging.info(" [x] Received %r" % body)
    process_batch_order(body)
    logging.info(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=config.SENSORY_SERVICE_RABBITMQ_BATCH_REQUEST_TASK_QUEUE)

channel.start_consuming()
