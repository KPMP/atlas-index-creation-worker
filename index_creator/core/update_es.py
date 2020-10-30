from elasticsearch import Elasticsearch

def update_file_cases(update_statement):
    elastic = Elasticsearch(["elasticsearch"])
    print(update_statement)
    elastic.bulk(update_statement)

