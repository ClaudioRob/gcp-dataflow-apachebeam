# consumer

import csv
import time
from google.cloud import pubsub_v1
import os


service_account_key = r"/home/claudio/udemy/gcp-dataflow-apachebeam/prj-gcp-dataflow-apachebeam-dd738740707b.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

subscription = 'projects/prj-gcp-dataflow-apachebeam/subscriptions/meusvoos-sub'
subscriber = pubsub_v1.SubscriberClient()

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack()

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)