# Atlas Index Creation Worker

The Atlas Index Creation Worker queries the data in the Staging Database and generates and submits the index updates to the Atlas File Repository’s Elasticsearch index. There are three endpoints for the Worker:

Regenerate the entire ES index:
http://localhost:5001/api/v1/index/file_cases

Update a single file:
http://localhost:5001/api/v1/index/file_cases/file_id/<file uuid>

Regenerate a given release:
http://localhost:5001/api/v1/index/file_cases/release_ver/<release version>


## Useful ES calls
This is a small subset of the entire API available for use. You may execute these restful queries as curl commands or through the kibana dev-tools panel. The following examples will assume the dev-tool panel is being used.
  
### Show List of Index
`GET _cat/indices/%2A?v=`

  ### Get the total number of documents within a specific index (file_cases)
`GET file_cases/_count`

 ### Show the type mapping for an index (file_cases)
`GET file_cases/_mapping/?include_type_name=true`

### Show the type mapping for a specific field (file_size) within an index (file_cases)
`GET /file_cases/_mapping/field/file_size?pretty`

  ### Examine the data of a single document within a specific instance
`GET file_cases/_doc/0043e10d-1f57-47be-a8c8-f97537efced8`
 
  
### Create a new Index,
```
PUT /file_cases?pretty
{
    "settings" : {
        "number_of_shards" : 1
    },
   "mappings": { 
     "properties": { 
		   "file_name": { "type": "keyword" },
       ...}
  }
}
```

### Transfer data from one index to another
```
  POST _reindex 
  {
    "source": {
         "index": "file_cases"
     },
     "dest": {
         "index": "file_cases_v2"
     }
}
```
  
### Delete an index
`DELETE file_cases`


## How to update the index

Elastic search does not allow an index to be updated, and this presents a challenge when the underlying model requires an update—For example, the transformation of a field from one type to another. The following is an example on updating a field, field_size, from type keyword to type long.

0. Before starting this process, ensure you have up-to-date backups in case something goes wrong.
  
1. Modify the existing index mapping
  `"file_name": { "type": "keyword" } =>  "file_name": { "type": "long" }`
  
2. Create a new index using the updated mapping. Note that this index must be a unique name. For our example, we are adding _temp to the name so that file_cases becomes file_cases_temp.
  
  ```
  PUT /file_cases_temp?pretty
  < mapping_json_here >
  ````
    
3. Transfer the data from the current index into the newly created index
```
    POST _reindex 
  {
    "source": {
         "index": "file_cases"
     },
     "dest": {
         "index": "file_cases_temp"
     }
}
```

4. Delete your current index   
`DELETE file_cases`

5. Reindex your newly created index to the name of your initial index.   
```
POST _reindex 
  {
    "source": {
         "index": "file_cases_temp"
     },
     "dest": {
         "index": "file_cases"
     }
}
```

6. Delete your temporary index    
`DELETE file_cases_v2`
 

7. You may need to update other services, such as arranger. To do so, start by tunneling to the knowledge environment with ports 8080.
`ssh -L 8080:localhost:8080 ssh atlas-ke`
    
8. Turn on the arranger-ui
`cd heavens-docker/atlas/knowledge-environment/ && docker-compose -f docker-compose.arranger_ui.yml up -d`
    
9. In your browser, navigate to localhost:8080.

10. Choose the project you're working on and make any required edits. In our example, we need to choose `endpoint` -> `File` -> and then update the DOI field to use `isArray`
    
11. We must create a temporary project to swap it with our current project, like we needed to do with the index itself. Arranger has some quirks, as such, it is best to name your temporary project with something like `123temp`
    
12. Delete your current project
    
13. Save your temporary project, creating a new project in the process with the name of your initial project.
    
14. Delete your temporary project.
  
15. We now need to restart the knowledge-environment to see the changes
`docker-compose -f docker-compose.prod.yml down && docker-compose -f docker-compose.prod.yml up -d`

## How to generate a dump index

This will pull data from the knowledge-environment to check what data is available. This will only make a GET request to the knowledge-environment to get the number of files associated with a version number.

This script will not work properly inside of the container. In some columns there will be a `bytearray(b"<cell contents>")` surrounding the content of the cell. 

1. Tunnel to the knowledge-environment machine and set port as 3306

2. On your local machine, copy the .env_example and rename to .env

3. Edit the .env and change the values to the correct user, and password

4. Change the .env host variable to 127.0.0.1

5. Run the `dry_run.py` script

6. If you want files for a specific version. `python3 dry_run.py -v <version>` Replace `<version>` with the version number of your choice.

7. If you want to get all files regardless of version. `python3 dry_run.py`