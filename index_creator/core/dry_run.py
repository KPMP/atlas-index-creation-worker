import mysql.connector
import os
import pandas as pd
import sys
import getopt
def input_version_number(argv):
    arg_ver = ""
    arg_help = "{0} -v <version>".format(argv[0])
    
    try:
        opts, args = getopt.getopt(argv[1:], "h:v:", ["help", "version="])
    except:
        print(arg_help)
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-v", "--version"):
            arg_ver = arg
    if arg_ver == "":
      generate_index()
      
    else:
      generate_index(None, arg_ver)
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
        where_clause = " WHERE arf.release_sunset_version is NULL ";
        if file_id is not None:
            where_clause = where_clause + " AND f.dl_file_id = '" + str(file_id) + "' "
        elif release_ver is not None:
            where_clause = where_clause + " AND arf.release_version = " + str(release_ver) + " "

        query = pd.read_sql_query("SELECT f.dl_file_id, p.redcap_id, p.sample_type, p.tissue_type, "
                "p.age_binned, p.sex, d.doi, m.access, m.platform, m.experimental_strategy, "
                "m.data_category, m.workflow_type, m.data_format, m.data_type, "
                "f.file_name, f.file_size, p.protocol, f.package_id, p.tissue_source "
                "FROM file f JOIN file_participant fp ON f.file_id = fp.file_id "
                "JOIN participant p ON fp.participant_id = p.participant_id "
                "LEFT JOIN doi_files fd ON f.file_id = fd.file_id "
                "LEFT JOIN doi d ON fd.doi_id = d.doi_id "
                "JOIN ar_file_info arf ON f.file_id = arf.file_id "
                "JOIN metadata_type m ON arf.metadata_type_id = m.metadata_type_id " + where_clause +
                "ORDER BY f.file_id", mydb)

        csv_file_path = './ke_dump.csv'
        df1 = pd.DataFrame(query)
        
        if release_ver is not None:
            query = pd.read_sql_query("SELECT afi.release_version, COUNT(f.file_id) as file_count FROM file f JOIN ar_file_info afi ON f.file_id = afi.file_id WHERE afi.release_version = " + release_ver, mydb)
        else:
            query = pd.read_sql_query("SELECT afi.release_version, COUNT(f.file_id) as file_count FROM file f JOIN ar_file_info afi ON f.file_id = afi.file_id GROUP BY afi.release_version", mydb)
        df2 = pd.DataFrame(query)
        df3 = pd.concat([df1, df2], axis=1)
        df3.to_csv(csv_file_path, index=False)
        
  finally:
    mydb.close()
if __name__ == "__main__":
    input_version_number()