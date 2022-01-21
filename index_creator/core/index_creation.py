import json
import mysql.connector
import os
from index_creator.models.FileCasesIndexDoc import FileCasesIndexDoc
from index_creator.models.IndexDoc import IndexDoc
import logging

log = logging.getLogger('index-creation-worker.index_creation')

def get_index_delete_json(id):
    return '{"delete":{"_index":"file_cases","_id":"' + str(id) + '"}}'

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

def generate_updates(mydb, file_id = None, release_ver = None):
    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)

        where_clause = " WHERE arf.release_sunset_version is NULL ";
        if file_id is not None:
            where_clause = where_clause + " AND f.dl_file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = where_clause + " AND arf.release_version = " + str(release_ver) + " "

        query = ("SELECT f.*, p.*, m.*, d.doi, arf.* FROM file f "
                 "JOIN file_participant fp on f.file_id = fp.file_id "
                 "JOIN participant p on fp.participant_id = p.participant_id "
                 "LEFT JOIN doi_files fd on f.file_id = fd.file_id "
                 "LEFT JOIN doi d on fd.doi_id = d.doi_id "
                 "JOIN ar_file_info arf ON f.file_id = arf.file_id "
                 "JOIN metadata_type m on f.metadata_type_id = m.metadata_type_id " + where_clause +
                 "order by f.file_id")
        mycursor.execute(query)
        documents = {}
        if not mycursor.rowcount:
            log.warning("query returned 0 results. No updates to process")
            return "";

        update_statement = '';
        for row in mycursor:

            if row["dl_file_id"] in documents:
                index_doc = documents[row["dl_file_id"]]
                # Not adding a new tissue source because we should only have one tissue source per file
                index_doc.cases.samples["participant_id"].append(row['redcap_id'])
                index_doc.cases.samples["sample_type"].append(row['sample_type'])
                index_doc.cases.samples["tissue_type"].append(row['tissue_type'])
                index_doc.cases.demographics["age"].append(row['age_binned'])
                index_doc.cases.demographics["sex"].append(row['sex'])
                index_doc.dois.append(row['doi'])
            else:
                cases_doc = FileCasesIndexDoc([row['tissue_source']], {"participant_id": [row['redcap_id']],
                                                                       "tissue_type": [row['tissue_type']],
                                                                       "sample_type": [row['sample_type']]},
                                              {"sex": [row['sex']], "age": [row['age_binned']]})
                index_doc = IndexDoc(row["access"], row["platform"], row["experimental_strategy"], row["data_category"],
                                     row["workflow_type"], row["data_format"], row["data_type"], row["dl_file_id"],
                                     row["file_name"], row["file_size"], row["protocol"], row["package_id"], [row["doi"]], cases_doc)
                documents[row["dl_file_id"]] = index_doc

        for id in documents:
            update_statement = update_statement + get_index_update_json(id) + "\n" + get_index_doc_json(
                documents[id]) + "\n"
        try:
            index_doc
            return update_statement
        except NameError:
            log.error("Unable to process results");
            pass
    finally:
        mycursor.close()

def get_max_release_ver(mydb):
    max_release_ver = 0.0;
    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)
        query = "SELECT MAX(release_ver) AS max FROM file"
        mycursor.execute(query)

        if not mycursor.rowcount:
            log.error("Unable to determine latest release version, deletes will not be processed")
            pass
        else:
            record = mycursor.fetchone()
            max_release_ver = record["max"]
    finally:
        mycursor.close()

    return max_release_ver;

def generate_deletes(mydb, file_id = None, release_ver = None):
    delete_statements = "";
    release_sunset_val = "";
    if release_ver is not None:
        release_sunset_val = release_ver;
    else:
        max_release_ver = get_max_release_ver(mydb);
        if max_release_ver is not None:
            release_sunset_val = str(max_release_ver);
        else:
            raise Exception("Cannot process deletes, index will not be update correctly");

    where_clause = " WHERE release_sunset ='" + release_sunset_val + \
                   "' AND dl_file_id NOT IN (SELECT dl_file_id FROM file " + \
                   "GROUP BY dl_file_id HAVING count(dl_file_id) > 1 ) ";
    if file_id is not None:
        where_clause = where_clause + " AND dl_file_id = '" + str(file_id) + "' "

    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)
        query = ("SELECT dl_file_id FROM file" + where_clause)
        mycursor.execute(query)

        if not mycursor.rowcount:
            log.info("0 records found to delete")
            pass

        for row in mycursor:
            delete_statements = delete_statements + get_index_delete_json(row['dl_file_id']) + "\n";

    finally:
        mycursor.close()

    return delete_statements;

def generate_index(file_id = None, release_ver = None):
    mysql_user = os.environ.get('MYSQL_USER')
    mysql_pwd = os.environ.get('MYSQL_ROOT_PASSWORD')
    mysql_host = os.environ.get('MYSQL_HOST')

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pwd,
        database="knowledge_environment",
        converter_class=MyConverter
    )
    try:
        es_update_statement = generate_updates(mydb, file_id, release_ver);
        es_update_statement = es_update_statement + generate_deletes(mydb, file_id, release_ver);
    finally:
        mydb.close();

    return es_update_statement;

class MyConverter(mysql.connector.conversion.MySQLConverter):

    def row_to_python(self, row, fields):
        row = super(MyConverter, self).row_to_python(row, fields)

        def to_unicode(col):
            if isinstance(col, bytearray):
                return col.decode('utf-8')
            return col

        return[to_unicode(col) for col in row]
