import json
import mysql.connector
import os
from index_creator.models.FileCasesIndexDoc import FileCasesIndexDoc
from index_creator.models.IndexDoc import IndexDoc
import logging

log = logging.getLogger('index-creation-worker.index_creation')

def get_index_update_json(id):
    return '{"update":{"_index":"file_cases","_id":"' + str(id) + '"}}'

def get_index_doc_json(index_doc):
    try:
        index_doc.cases = index_doc.cases.__dict__
        json_doc = json.dumps(index_doc.__dict__)
        doc = '{"doc":' + json_doc + ',"doc_as_upsert":true}'
    except TypeError as err:
        log.error(err)
    return doc

def generate_index(file_id = None, release_ver = None):
    mysql_user = os.environ.get('MYSQL_USER')
    mysql_pwd = os.environ.get('MYSQL_ROOT_PASSWORD')

    mydb = mysql.connector.connect(
        host="mariadb",
        user=mysql_user,
        password=mysql_pwd,
        database="knowledge_environment",
        converter_class=MyConverter
    )
    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)

        where_clause = ""
        if file_id is not None:
            where_clause = " WHERE f.file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = " WHERE f.release_ver = " + str(release_ver) + " "

        query = ("SELECT f.*, p.*, m.* FROM file f "  
                  "JOIN file_participant fp on f.file_id = fp.file_id "
                  "JOIN participant p on fp.participant_id = p.participant_id "
                  "JOIN metadata_type m on f.metadata_type_id = m.metadata_type_id " + where_clause +
                  "order by f.file_id")

        mycursor.execute(query)
        documents = {}

        update_statement = '';
        for row in mycursor:
            # log.debug('age type: ' + str(type(row['age_binned'])))
            # log.debug('tissue source type: ' + str(type(row['tissue_source'])))
            # log.debug('participant id type: ' + str(type(row['participant_id'])))
            # log.debug('tissue type type: ' + str(type(row['tissue_type'])))
            # log.debug('sample type type: ' + str(type(row['sample_type'])))
            # log.debug('sex type: ' + str(type(row['sex'])))
            if row["file_id"] in documents:
                index_doc = documents[row["file_id"]]
                # Not adding a new tissue source because we should only have one tissue source per file
                index_doc.cases.samples["participant_id"].append(row['participant_id'])
                index_doc.cases.samples["sample_type"].append(row['sample_type'])
                index_doc.cases.samples["tissue_type"].append(row['tissue_type'])
                index_doc.cases.demographics["age"].append(row['age_binned'])
                index_doc.cases.demographics["sex"].append(row['sex'])
            else:
                cases_doc = FileCasesIndexDoc([row['tissue_source']], {"participant_id":[row['participant_id']], "tissue_type":[row['tissue_type']], "sample_type":[row['sample_type']]},{"sex":[row['sex']], "age":[row['age_binned']]})
                index_doc = IndexDoc(row["access"], row["platform"], row["experimental_strategy"], row["data_category"], row["workflow_type"], row["data_format"], row["data_type"], row["file_id"], row["file_name"], row["file_size"], row["protocol"], row["package_id"], cases_doc)
                documents[row["file_id"]] =  index_doc

        for id in documents:
            update_statement = update_statement + get_index_update_json(id) + "\n" + get_index_doc_json(documents[id]) + "\n"
        try:
            index_doc
            return update_statement
        except NameError:
            log.error("No records matched query: " + query);
            pass
    finally:
        mycursor.close()
        mydb.close()

class MyConverter(mysql.connector.conversion.MySQLConverter):

    def row_to_python(self, row, fields):
        row = super(MyConverter, self).row_to_python(row, fields)

        def to_unicode(col):
            if isinstance(col, bytearray):
                return col.decode('utf-8')
            return col

        return[to_unicode(col) for col in row]