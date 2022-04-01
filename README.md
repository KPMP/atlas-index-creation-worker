Atlas Index Creation Worker

The Atlas Index Creation Worker queries the data in the Staging Database and generates and submits the index updates to the Atlas File Repository’s Elasticsearch index. There are three endpoints for the Worker:

Regenerate the entire ES index:
http://localhost:5001/api/v1/index/file_cases

Update a single file:
http://localhost:5001/api/v1/index/file_cases/file_id/<file uuid>

Regenerate a given release:
http://localhost:5001/api/v1/index/file_cases/release_ver/<release version>


# Index Mapping

The index mapping file, containing the field/type relationships, are located within the knowledge-environment repository.