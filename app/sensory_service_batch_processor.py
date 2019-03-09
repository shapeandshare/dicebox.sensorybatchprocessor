#!flask/bin/python
###############################################################################
# Sensory Batch Processor
#   Handles requests for batch sensory data.  This service acts as a back-
# ground processor, and interacts with the RabbitMQ message system and the local
# filesystem only.
#
# Copyright (c) 2017-2019 Joshua Burt
###############################################################################


###############################################################################
# Dependencies
###############################################################################
import lib.docker_config as config
from lib import filesystem_connecter
import logging
import json
import pika
import numpy
import os
import errno

###############################################################################
# Allows for easy directory structure creation
# https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
###############################################################################
def make_sure_path_exists(path):
    try:
        if os.path.exists(path) is False:
            os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


###############################################################################
# Setup logging.
###############################################################################
make_sure_path_exists(config.LOGS_DIR)
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.DEBUG,
    filemode='w',
    filename="%s/%s.sensory_service_batch_processor.log" % (config.LOGS_DIR, os.uname()[1])
)


###############################################################################
# Setup logging
###############################################################################
logging.debug("creating a new fsc..")
fsc = filesystem_connecter.FileSystemConnector(config.DATA_DIRECTORY)


###############################################################################
# Message System Configuration
###############################################################################
url = config.SENSORY_SERVICE_RABBITMQ_URL
parameters = pika.URLParameters(url)
connection = pika.BlockingConnection(parameters=parameters)

channel = connection.channel()

# channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue=config.SENSORY_SERVICE_RABBITMQ_BATCH_REQUEST_TASK_QUEUE, durable=True)
channel.basic_qos(prefetch_count=1)


###############################################################################
# Batch Order Processing Logic
###############################################################################
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
    arguments = {'x-expires': 1800 * 1000}  # 1800 seconds = 30 minutes
    uuid_channel.queue_declare(queue=sensory_batch_request_id, durable=False, auto_delete=True, arguments=arguments)

    uuid_channel.queue_bind(sensory_batch_request_id, config.SENSORY_SERVICE_RABBITMQ_EXCHANGE, routing_key=sensory_batch_request_id, arguments=None)

    for index in range(0, len(image_labels)):
        outbound_message = json.dumps({'label': numpy.array(image_labels[index]).tolist(), 'data': numpy.array(image_data[index]).tolist()})
        uuid_channel.basic_publish(exchange=config.SENSORY_SERVICE_RABBITMQ_EXCHANGE,
                                   routing_key=sensory_batch_request_id,
                                   body=outbound_message
                                   )
    outbound_connection.close()





###############################################################################
# Our callback when message consumption is ready to occur
###############################################################################
def callback(ch, method, properties, body):
    logging.info(" [x] Received %r" % body)
    process_batch_order(body)
    logging.info(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)


###############################################################################
# Main wait loop begins now ..
###############################################################################
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.basic_consume(callback,
                      queue=config.SENSORY_SERVICE_RABBITMQ_BATCH_REQUEST_TASK_QUEUE)
channel.start_consuming()
