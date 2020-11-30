from elasticsearch import Elasticsearch, ConnectionError, ElasticsearchException
import logging

log = logging.getLogger('index-creation-worker.update_file_cases')

def update_file_cases(update_statement):
    try:
        elastic = Elasticsearch(['elasticsearch'])
        elastic.bulk(update_statement)
    except ConnectionError as connectionError:
        log.error("error communicating with elasicsearch: " + str(connectionError))
    except ElasticsearchException as e:
        log.error("other error with elasticsearch: " + str(e))

