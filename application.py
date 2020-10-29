import flask
from flask import request, jsonify
from index_creator.core.index_creation import *

app = flask.Flask(__name__)

@app.route('/api/v1/index/file_cases', methods=['GET'])
def updateFileCases():

    return jsonify(generate_index())

app.run()
