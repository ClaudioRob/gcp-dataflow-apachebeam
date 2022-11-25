#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

service_account_key = r"/home/claudio/udemy/gcp-dataflow-apachebeam/prj-gcp-dataflow-apachebeam-dd738740707b.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

topico = 'projects/prj-gcp-dataflow-apachebeam/topics/meusvoos'
publisher = pubsub_v1.PublisherClient()

entrada = r"/home/claudio/udemy/gcp-dataflow-apachebeam/Python-Apache-Beam/voos_sample.csv"     

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico,row)
        time.sleep(2)