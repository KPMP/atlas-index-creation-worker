import flask
import sys
from index_creator.core.index_creation import generate_index
from index_creator.core.update_es import update_file_cases
import logging

app = flask.Flask('index-creation-worker')

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
app.logger.addHandler(handler)
app.logger.setLevel(logging.DEBUG)

@app.route('/api/v1/index/file_cases', methods=['PUT'])
def updateFileCases():
    try:
        update_statement = generate_index()
        update_file_cases(update_statement)
        return "ok - updated with all files"
    except:
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

@app.route('/api/v1/index/file_cases/file_id/<string:file_id>', methods=['PUT'])
def updateFileCase(file_id):
    try:
        update_statement = generate_index(file_id=file_id)
        app.logger.debug(update_statement)
        update_file_cases(update_statement)
        return "ok - updated with file: " + str(file_id)
    except:
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

@app.route('/api/v1/index/file_cases/release_ver/<string:release_ver>', methods=['PUT'])
def updateFileCaseRelease(release_ver):
    try:
        update_statement = generate_index(release_ver=release_ver)
        update_file_cases(update_statement)
        return "ok - updated release: " + release_ver
    except:
        app.logger.error(str(sys.exc_info()[0]) + " on line: " + str(sys.exc_info()[-1].tb_lineno))
        return "There was an error updating the index. Check the logs"

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
