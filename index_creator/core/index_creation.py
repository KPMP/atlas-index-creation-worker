import json
import mysql.connector
import os
from index_creator.models.FileCasesIndexDoc import FileCasesIndexDoc
from index_creator.models.EnterpriseSearchIndexDoc import EnterpriseSearchIndexDoc
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
        index_doc.dois = list(index_doc.dois)
        json_doc = json.dumps(index_doc.__dict__)
        doc = '{"doc":' + json_doc + ',"doc_as_upsert":true}'
    except TypeError as err:
        log.error(err)
    return doc

def get_enterprise_index_json(index_doc):
    try:
        index_doc.dois = list(index_doc.dois)
        index = {}
        for key in index_doc.__dict__:
            if index_doc.__dict__[key] != "" and type(index_doc.__dict__[key]) != list and index_doc.__dict__[key] != None:
                index[key] = index_doc.__dict__[key]
            if type(index_doc.__dict__[key]) == list:
                str_list = list(filter(None, index_doc.__dict__[key]))
                if len(str_list) > 0:
                    index[key] = str_list
        json_doc = json.dumps(index)
        return json_doc
    except TypeError as err:
        log.error(err)


def generate_json(mydb, file_id = None, release_ver = None):
    try:
        values = ()
        mycursor = mydb.cursor(buffered=True, dictionary=True)

        where_clause = " WHERE arf.release_sunset_version is NULL ";
        if file_id is not None:
            where_clause = where_clause + " AND f.dl_file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = where_clause + " AND arf.release_version = CAST(%s AS FLOAT) "
            values = (str(release_ver),)

        query = ("SELECT f.dl_file_id, p.redcap_id, p.sample_type, p.tissue_type, "
                "p.age_binned, p.sex, d.doi, m.access, m.platform, m.experimental_strategy, "
                "m.data_category, m.workflow_type, m.data_format, m.data_type, "
                "f.file_name, f.file_size, p.protocol, f.package_id, p.tissue_source "
                "FROM file f JOIN file_participant fp ON f.file_id = fp.file_id "
                "JOIN participant p ON fp.participant_id = p.participant_id "
                "LEFT JOIN doi_files fd ON f.file_id = fd.file_id "
                "LEFT JOIN doi d ON fd.doi_id = d.doi_id "
                "JOIN ar_file_info arf ON f.file_id = arf.file_id "
                "JOIN metadata_type m ON arf.metadata_type_id = m.metadata_type_id " + where_clause +
                " ORDER BY f.file_id");

        mycursor.execute(query, values)
        documents = {}
        if not mycursor.rowcount:
            log.warning("query returned 0 results. No updates to process")
            return "";

        for row in mycursor:

            # If we already have a document for this file, add information to it
            if row["dl_file_id"] in documents:
                index_doc = documents[row["dl_file_id"]]
                # Not adding a new tissue source because we should only have one tissue source per file
                index_doc.redcap_id.append(row['redcap_id'])
                index_doc.sample_type.append(row['sample_type'])
                index_doc.tissue_type.append(row['tissue_type'])
                index_doc.protocol.append(row['protocol'])
                index_doc.age_binned.append(row['age_binned'])
                index_doc.sex.append(row['sex'])
                index_doc.tissue_source.append(row['tissue_source'])
                index_doc.dois.add(row['doi'])
                index_doc.participant_id_sort = ["Multiple Participants"]
                index_doc.file_name_sort = row['file_name'][37:]
                index_doc.platform_sort = "aaaaa" if not row['platform'] else row['platform']
            # If this is a new file, then we need to create the initial record and add it to our list of documents
            else:
                index_doc = EnterpriseSearchIndexDoc(row["access"], row["platform"], row["experimental_strategy"], row["data_category"],
                                     row["workflow_type"], row["data_format"], row["data_type"], row["dl_file_id"],
                                     row["file_name"], row["file_size"], row["package_id"], {row["doi"]}, [row['redcap_id']], [row['sample_type']],
                                     [row['tissue_type']], [row['protocol']], [row['sex']], [row['age_binned']], [row['tissue_source']])
                documents[row["dl_file_id"]] = index_doc

        update_statement = '[';
        for id in documents:
            update_statement = update_statement + get_enterprise_index_json(documents[id]) + ","
        try:
            return update_statement + ']'
        except NameError:
            log.error("Unable to process results");
            pass
    finally:
        mycursor.close()

def generate_updates(mydb, file_id = None, release_ver = None):
    try:
        values = ()
        mycursor = mydb.cursor(buffered=True, dictionary=True)

        where_clause = " WHERE arf.release_sunset_version is NULL ";
        if file_id is not None:
            where_clause = where_clause + " AND f.dl_file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = where_clause + " AND arf.release_version = CAST(%s AS FLOAT) "
            values = (str(release_ver),)

        query = ("SELECT f.dl_file_id, p.redcap_id, p.sample_type, p.tissue_type, "
                "p.age_binned, p.sex, d.doi, m.access, m.platform, m.experimental_strategy, "
                "m.data_category, m.workflow_type, m.data_format, m.data_type, "
                "f.file_name, f.file_size, p.protocol, f.package_id, p.tissue_source "
                "FROM file f JOIN file_participant fp ON f.file_id = fp.file_id "
                "JOIN participant p ON fp.participant_id = p.participant_id "
                "LEFT JOIN doi_files fd ON f.file_id = fd.file_id "
                "LEFT JOIN doi d ON fd.doi_id = d.doi_id "
                "JOIN ar_file_info arf ON f.file_id = arf.file_id "
                "JOIN metadata_type m ON arf.metadata_type_id = m.metadata_type_id " + where_clause +
                "ORDER BY f.file_id");
        mycursor.execute(query, values)
        documents = {}
        if not mycursor.rowcount:
            log.warning("query returned 0 results. No updates to process")
            return "";

        update_statement = '';
        for row in mycursor:

            # If we already have a document for this file, add information to it
            if row["dl_file_id"] in documents:
                index_doc = documents[row["dl_file_id"]]
                # Not adding a new tissue source because we should only have one tissue source per file
                index_doc.cases.samples["participant_id"].append(row['redcap_id'])
                index_doc.cases.samples["sample_type"].append(row['sample_type'])
                index_doc.cases.samples["tissue_type"].append(row['tissue_type'])
                index_doc.cases.samples["protocol"].append(row['protocol'])
                index_doc.cases.demographics["age"].append(row['age_binned'])
                index_doc.cases.demographics["sex"].append(row['sex'])
                index_doc.cases.tissue_source.append(row['tissue_source'])
                index_doc.dois.add(row['doi'])
            # If this is a new file, then we need to create the initial record and add it to our list of documents
            else:
                cases_doc = FileCasesIndexDoc([row['tissue_source']],
                                              {"participant_id": [row['redcap_id']],
                                                "tissue_type": [row['tissue_type']],
                                                "sample_type": [row['sample_type']],
                                                "protocol": [row['protocol']]},
                                                {"sex": [row['sex']], "age": [row['age_binned']]})
                index_doc = IndexDoc(row["access"], row["platform"], row["experimental_strategy"], row["data_category"],
                                     row["workflow_type"], row["data_format"], row["data_type"], row["dl_file_id"],
                                     row["file_name"], row["file_size"], row["package_id"], {row["doi"]}, cases_doc)
                documents[row["dl_file_id"]] = index_doc

        # Now that we have all of our documents, create the update statement to run in ES
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
        query = "SELECT MAX(release_version) AS max FROM ar_file_info"
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
        release_sunset_val = str(release_ver);
    else:
        max_release_ver = get_max_release_ver(mydb);
        if max_release_ver is not None:
            release_sunset_val = str(max_release_ver);
        else:
            raise Exception("Cannot process deletes, index will not be update correctly");

    where_clause = " WHERE arf.release_sunset_version = CAST(%s AS DECIMAL) AND dl_file_id NOT IN (SELECT dl_file_id FROM file " + \
                   "GROUP BY dl_file_id HAVING count(dl_file_id) > 1 ) ";
    if file_id is not None:
        where_clause = where_clause + " AND dl_file_id = '" + str(file_id) + "' "

    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)
        query = ("SELECT dl_file_id FROM file f JOIN ar_file_info arf ON f.file_id = arf.file_id " + where_clause)
        mycursor.execute(query, (release_sunset_val,))

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

def generate_repo_index():
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
        update_statement = generate_json(mydb)
    finally:
        mydb.close()

    return update_statement


class MyConverter(mysql.connector.conversion.MySQLConverter):

    def row_to_python(self, row, fields):
        row = super(MyConverter, self).row_to_python(row, fields)

        def to_unicode(col):
            if isinstance(col, bytearray):
                return col.decode('utf-8')
            return col

        return[to_unicode(col) for col in row]
