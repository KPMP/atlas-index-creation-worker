import flask
from flask import request, jsonify
from index_creator.core.index_creation import generate_index
from index_creator.core.update_es import update_file_cases

app = flask.Flask(__name__)

@app.route('/api/v1/index/file_cases', methods=['GET'])
def updateFileCases():
    update_file_cases(generate_index())
    return "ok"

app.run()
