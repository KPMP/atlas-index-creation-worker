import flask
from index_creator.core.index_creation import generate_index
from index_creator.core.update_es import update_file_cases

app = flask.Flask(__name__)

@app.route('/api/v1/index/file_cases', methods=['PUT'])
def updateFileCases():
    update_file_cases(generate_index())
    return "ok"

@app.route('/api/v1/index/file_cases/file_id/<string:file_id>', methods=['PUT'])
def updateFileCase(file_id):
    update_file_cases(generate_index(file_id=file_id))
    return "ok"

@app.route('/api/v1/index/file_cases/release_ver/<string:release_ver>', methods=['PUT'])
def updateFileCaseRelease(release_ver):
    update_file_cases(generate_index(release_ver=release_ver))
    return "ok"

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
