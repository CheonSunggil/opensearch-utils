import boto3
import time

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from tqdm import tqdm

class OpenSearcClient(object):
    def __init__(self, HOST, PORT, REGION, AWS_ACCESSKEY, AWS_SECRETKEY):
            
        self.HOST = HOST
        self.PORT = PORT
        self.REGION = REGION
        
        self.index_name = ''
        
        
        credentials = boto3.Session(
            aws_access_key_id = AWS_ACCESSKEY, 
            aws_secret_access_key = AWS_SECRETKEY
        ).get_credentials()
        
        auth = AWSV4SignerAuth(credentials, self.REGION)
        
        self.client = OpenSearch(
                        hosts = [{'host': self.HOST, 'port': self.PORT}],
                        http_auth = auth,
                        use_ssl = True,
                        verify_certs = True,
                        connection_class = RequestsHttpConnection
        )
        
    def make_index(self, index_name, index_mapping, overwrite=True):

        if self.client.indices.exists(index=index_name):
            if overwrite==True :
                self.client.indices.delete(index=index_name)
                self.client.indices.create(index=index_name, body=index_mapping)
                print('{} 이 기존에 존재하므로 제거 후, 생성 성공!'.format(index_name))
            else :
                print('{} 이 기존에 존재하여 생성 실패!'.format(index_name))
        else :
            self.client.indices.create(index=index_name, body=index_mapping)
            print('{} 생성 성공 !'.format(index_name))

        self.index_name = index_name

    def index_batch(self, docs, index_name):

        if index_name== False and self.index_name != '' :
            index_name = self.index_name

        index_type = '_doc'
        actions = []
        for doc  in docs:
            action = {
                "index":{
                    "_index": index_name,
                }
            }

            actions.append(action)
            actions.append(doc)
        self.client.bulk(actions, refresh=True)

    def run_bulk_index(self, document, batch_size=500, index_name=''):

        if index_name== '' and self.index_name != '' :
            index_name = self.index_name

        batch = []
        start = time.time()
        for i, doc in enumerate(tqdm(document)):
            batch.append(doc)
            if len(batch) == batch_size:
                self.index_batch(batch, index_name)
                batch = []

        if batch :
            self.index_batch(batch, index_name)
        print(f"Runtime: {time.time()-start:.4f} sec") 

        self.check_index_capacity(index_name)
        self.check_num_document(index_name)

    def check_index_capacity(self, index_name) :
        index_name = 'product_embedding_vector_approximate_knn'

        print(self.client.cat.indices(index=index_name, bytes="gb"))

    def check_num_document(self, index_name):
        query = {
            "query": {
                "match_all": {}  
            }
        }
        count = self.client.count(index=index_name, body=query)['count']

        print(f"The number of indexed documents in index {index_name} is {count}.")