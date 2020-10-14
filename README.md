# Sports Data Bank

There are 2 steps to import the data from xml into CKAN. 

### Step 1 is converting xml to json ###
Run ```python sport_parser_xml_json.py``` will convert the xml files from designate folder to json files. Please use absolute file path for both input dir and output dir. 

### Step 2 is importing json into CKAN ###
Run ```python import_xml_to_ckan.py sdb True``` Please be noted that, it is using config file `importer-conf.json`, please edit it accordingly

### Known issues ###
```
SearchIndexError: Solr returned an error: (u'Solr responded with an error (HTTP 400): [Reason: Exception writing document id d76b1e485361589265e07d87b8a8a615 to the index; possible analysis error: Document contains at least one immense term in field="doelstelling" (whose UTF8 encoding is longer than the max length 32766), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: \'[34, 60, 112, 62, 72, 101, 116, 32, 32, 98, 101, 111, 101, 102, 101, 110, 101, 110, 32, 118, 97, 110, 32, 104, 101, 116, 32, 118, 111, 101]...\', original message: bytes can be at most 32766 in length; got 51931. Perhaps the document has an indexed string field (solr.StrField) which is too large]',)
```
Solr field too small for certain fields, pump it up in solr configure