import flask
import sys
from index_creator.core.index_creation import generate_index
from index_creator.core.index_creation import generate_repo_index
from index_creator.core.update_es import update_file_cases
import logging

app = flask.Flask('index-creation-worker')

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
app.logger.addHandler(handler)
app.logger.setLevel(logging.DEBUG)

@app.route('/api/v1/index/file_cases', methods=['GET', 'PUT']) #the POST method is what elasticSearch's _bulk() API expects
def updateFileCases():
    try:
        update_statement = generate_index()
        if update_statement is not None:
            update_file_cases(update_statement)
            return "ok - updated with all files"
        else:
            app.logger.warn("Index not updated, no results to apply")
            return "Index not updated, no results to apply"
    except Exception as e:
        app.logger.error(e)
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

@app.route('/api/v1/index/file_cases/file_id/<string:file_id>', methods=['PUT', 'GET'])
def updateFileCase(file_id):
    try:
        update_statement = generate_index(file_id=file_id)
        if update_statement is not None:
            update_file_cases(update_statement)
            return "ok - updated with file: " + str(file_id)
        else:
            app.logger.warn("Index not updated, no results to apply")
            return "Index not updated, no results to apply"
    except Exception as e:
        app.logger.error(e)
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

@app.route('/api/v1/index/file_cases/release_ver/<string:release_ver>', methods=['PUT', 'GET'])
def updateFileCaseRelease(release_ver):
    try:
        release_ver = float(release_ver)
        update_statement = generate_index(None, release_ver)
        if update_statement is not None:
            update_file_cases(update_statement)
            return "ok - updated release: " + str(release_ver)
        else:
            app.logger.warn("Index not updated, no results to apply")
            return "Index not updated, no results to apply"
    except Exception as e:
        app.logger.error(e)
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

@app.route('/api/v1/index/repository_index', methods=['GET'])
def generateEnterpriseSerchIndex():
    try:
        update_statement = generate_repo_index()
        if update_statement is not None:
            return update_statement
        else:
            app.logger.warn("Unable to generate new index")
            return "Unable to generate new index"
    except Exception as e:
        app.logger.error(e)
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error generating the index. Check the logs"

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
