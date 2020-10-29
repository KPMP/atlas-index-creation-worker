import json
import mysql.connector
from dotenv import load_dotenv
import os
from index_creator.models.FileCasesIndexDoc import FileCasesIndexDoc
from index_creator.models.IndexDoc import IndexDoc

def get_index_update_json(id):
    return '{"update":{"_index":"file_cases","_id":"' + id + '"}}'

def get_index_doc_json(index_doc):
    index_doc.cases = index_doc.cases.__dict__
    return '{"doc":' + json.dumps(index_doc.__dict__) + ',"doc_as_upsert":true}'

def generate_index():
    load_dotenv()

    mysql_user = os.environ.get('mysql_user')
    mysql_pwd = os.environ.get('mysql_pwd')

    mydb = mysql.connector.connect(
        host="localhost",
        user=mysql_user,
        password=mysql_pwd,
        database="knowledge_environment"
    )
    mycursor = mydb.cursor(buffered=True, dictionary=True)

    # where_clause = ""

    # parser = ArgumentParser(description="Generate ES index updates. No arguments will create updates for all records.")
    # parser.add_argument("-f", "--file_id", dest="file_id",
    #                     help="file ID")
    # parser.add_argument("-v", "--release_ver",
    #                     dest="release_ver",
    #                     help="target release version")
    #
    # args = parser.parse_args()
    #
    # if args.file_id:
    #     where_clause = " WHERE f.file_id = '" + args.file_id + "' "
    # elif args.release_ver:
    #     where_clause = " WHERE f.release_ver = " + args.release_ver + " "

    query = ("SELECT f.*, fp.*, p.*, m.* FROM file f "  
              "JOIN file_participant fp on f.file_id = fp.file_id "
              "JOIN participant p on fp.participant_id = p.participant_id "
              "JOIN metadata_type m on f.metadata_type_id = m.metadata_type_id")

    mycursor.execute(query)
    row_num = 1
    last_file_id = -1

    for row in mycursor:
        if row["file_id"] != last_file_id:
            if row_num != 1:
                print(get_index_update_json(index_doc.file_id) + "\n" +
                      get_index_doc_json(index_doc))
            cases_doc = FileCasesIndexDoc([row['tissue_source']], {"participant_id":[row['participant_id']], "tissue_type":[row['tissue_type']], "sample_type":[row['sample_type']]},{"sex":[row['sex']], "age":[row['age_binned']]})
            index_doc = IndexDoc(row["access"], row["platform"], row["experimental_strategy"], row["data_category"], row["workflow_type"], row["data_format"], row["data_type"], row["file_id"], row["file_name"], row["file_size"], row["protocol"], row["package_id"], cases_doc)
        else:
            index_doc.cases.tissue_source.append(row['tissue_source'])
            index_doc.cases.samples["participant_id"].append(row['participant_id'])
            index_doc.cases.samples["sample_type"].append(row['sample_type'])
            index_doc.cases.samples["tissue_type"].append(row['tissue_type'])
            index_doc.cases.demographics["age"].append(row['age_binned'])
            index_doc.cases.demographics["sex"].append(row['sex'])
        row_num += 1
        last_file_id = row["file_id"]

    # print(get_index_update_json(index_doc.file_id) + "\n" +
    #       get_index_doc_json(index_doc))
    try:
        index_doc
        return get_index_update_json(index_doc.file_id) + "\n" + get_index_doc_json(index_doc)
    except NameError:
        pass
