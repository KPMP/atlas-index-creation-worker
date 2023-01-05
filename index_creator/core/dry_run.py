import mysql.connector
import os
def generate_index(file_id = None, release_ver = None):
    mysql_user = os.environ.get('MYSQL_USER')
    mysql_pwd = os.environ.get('MYSQL_ROOT_PASSWORD')
    mysql_host = os.environ.get('MYSQL_HOST')

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pwd,
        database="knowledge_environment"
    )
    try:
        generate_updates(mydb, file_id, release_ver);
    finally:
        mydb.close();

def generate_updates(mydb, file_id = None, release_ver = None):
  try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)

        where_clause = " WHERE arf.release_sunset_version is NULL ";
        if file_id is not None:
            where_clause = where_clause + " AND f.dl_file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = where_clause + " AND arf.release_version = " + str(release_ver) + " "

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

        mycursor.execute(query)
        header = [row[0] for row in mycursor.description]
        rows = mycursor.fetchall()
        f = open("./ke_dump","w")
        f.write(",".join(header) + '\n')
        
        for row in rows:
            f.write(",".join(str(r) for r in row) + "\n")
        f.close()
        print(str(len(rows)) + " rows written successfully to " + f.name)
  finally:
    mydb.close()