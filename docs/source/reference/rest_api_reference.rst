.. role:: gray

.. role:: code

.. role:: nowrap

.. raw:: html

    <style>
        .rst-content table.docutils td {
            vertical-align: top;
        }
        .wy-table-responsive table td, .wy-table-responsive table th {
            white-space: normal;
        }
        .gray {
            color: gray;
            font-style: italic;
        }
        .code {
            font-family: consolas;
        }
        .nowrap {
            white-space: nowrap;
        }
    </style>

.. |br| raw:: html

    <br />

.. |nbspc| unicode:: U+00A0 .. non-breaking space

.. |nbsp| unicode:: 0xA0 
   :trim:

.. _rest_api_reference:

REST API reference (v1)
=======================

The following tables document the HTTP endpoints exposed by a Polytope instance (under <SERVICE_URL>/api/v1), and their expected inputs and outputs. They are grouped in sections according to the action they are involved in.

List collections
----------------

Protocol for retrieving names of collections exposed by the Polytope instance.

**Step 1**: GET on /collections.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - GET
     - /collections
     - .. code:: bash

          Authorization: <string>
          Host: polytope.example.com
          
       | :gray:`For valid authorization headers see authentication and authorization.`
       | :gray:`Host is usually set automatically by your HTTP client.`
     - `-`
     - OK 200
     - | Headers:
       .. code-block:: bash
       
          Content-Type: application/json
       
       | Body: a JSON object (utf8)
       .. code-block:: bash

          {
            "message": ["<collection-name-1>", "<collection-name-2>", ...]
          }

Retrieve
--------

Protocol for retrieving data, by posting a request, polling its status while it is being processed, and downloading the result.

**Step 1**: POST a request to /requests/<COLLECTION>.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - POST
     - | /requests/<COLLECTION>
       | :gray:`<COLLECTION> is the name of one of the collections exposed by the Polytope server`
       | |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp|
     - .. code:: bash

          Authorization: <string>
          Accept: application/json
          Content-Type: application/json
          Content-Length: <length>
          Host: polytope.ecmwf.int

       | :gray:`For valid authorization headers see authentication and authorization.`
       | :gray:`Content-Length and Host are usually set automatically by your HTTP client.`
     - .. code::

          {
            "verb": "retrieve",
            "request": <request string>
          } 

       | :gray:`Request is usually JSON or YAML, but depends on the collection.`
     - ACC 202
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <POLLING_URL>
          Retry-After: <delay-seconds>
          
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status" : "failed" | "queued" | "processing",
            "message": <info>
          }

**Step 2**: GET on <POLLING_URL> repeatedly, until you receive 303.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - GET
     - | <POLLING_URL>
       | :gray:`From step 1. May be relative or absolute URL.`
     - :gray:`As in step 1, but without content-type and content-length.`
     - `-`
     - ACC 202
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <POLLING_URL>        # Not always present
          Retry-After: <delay-seconds>   # Not always present
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status": "failed" | "queued" | "processing",
            "message": <info>
          }
   * - 
     - 
     - 
     - 
     - SEE OTHER 303
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <DOWNLOAD_URL>
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "location": "<DOWNLOAD_URL>",
            "contentLength": <size>,
            "contentType": <type>
          }

**Step 3**: GET on <DOWNLOAD_URL> to receive data.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - GET
     - | <DOWNLOAD_URL>
       | :gray:`From step 2. May be relative or absolute URL.`
     - | `-`
       | :gray:`no authentication is required in order to download processed data`
     - `-`
     - OK 200
     - | Headers:
       .. code:: bash

          Content-Type: <string>
          Content-Length: <size>
          Content-MD5: <128-bit MD5 digest> # Not always present
       | Body:
       .. code:: bash

          Resultant data

Archive
-------

Protocol for archiving data by posting a request, posting the data, and polling its status until listed.

**Step 1**: POST a request to /requests/<COLLECTION>.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - POST
     - | /requests/<COLLECTION>
       | :gray:`<COLLECTION> is the name of one of the collections exposed by the Polytope server`
       | :gray:`e.g. fdb-test`
       | |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp|
     - .. code:: bash

          Authorization: <string>
          Accept: application/json
          Content-Type: application/json
          Content-Length: <length>
          Host: polytope.ecmwf.int
       | :gray:`For valid authorization headers see authentication and authorization.`
       | :gray:`Content-Length and Host are usually set automatically by your HTTP client.`
     - .. code::

          {
            "verb": "archive",
            "request": <request string>,
            "url": "<REQUEST_URL>"  # Optional
                # If specified, Polytope will 
                # pull data from this URL.
          }

        | :gray:`Request is usually JSON or YAML, but depends on the collection.`
     - ACC 202
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <POLLING_URL>
          Retry-After: <delay-seconds>
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status" : "failed" | "queued" | "processing",
            "message": <info>
          }

**Step 2**: POST on <POLLING_URL> to upload data. This step has to be skipped if a "url" to pull data from has been provided in the archive request in Step 1.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - POST
     - | <POLLING_URL>
       | :gray:`From step 1. May be relative or absolute URL.`
     - .. code:: bash

          Content-Type: <string>
          Content-Length: <size>
          Content-MD5: <128-bit MD5 digest>
       | :gray:`no authentication is required in order to upload data`
     - .. code:: bash

          Resultant data
     - ACC 202
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <POLLING_URL>
          Retry-After: <delay-seconds>
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status" : "failed" | "queued" | "processing",
            "message": <info>
          }

**Step 3**: GET on <POLLING_URL> repeatedly, until uploaded data is listed and you receive 200.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - GET
     - | <POLLING_URL>
       | :gray:`From step 1 or 2. May be relative or absolute URL.`
     - :gray:`As in step 1, but without content-type and content-length.`
     - `-`
     - ACC 202
     - | Headers:
       .. code:: bash

          Content-Type: application/json
          Location: <POLLING_URL>        # Not always present
          Retry-After: <delay-seconds>   # Not always present
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status": "failed" | "queued" | "processing",
            "message": <info>
          }
   * - 
     - 
     - 
     - 
     - OK 200
     - | Headers:
       .. code:: bash

          Content-Type: application/json
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "status": "processed",
            "message": <info>
          }

List requests
-------------

Protocol for listing all active user requests.

**Step 1**: GET on /requests.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - GET
     - /requests
     - .. code:: bash

          Authorization: <string>
          Host: polytope.ecmwf.int
       | :gray:`For valid authorization headers see authentication and authorization.`
       | :gray:`Host is usually set automatically by your HTTP client.`
     - `-`
     - OK 200
     - | Headers:
       .. code:: bash

          Content-Type: application/json
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "message": [
              {
                "id": "<request-id-1>",
                "timestamp": <timestamp-1>,
                "last_modified": <last-modified-1>,
                "user": {
                  "id": "<user-id>",
                  "username": "<username>",
                  "realm": "<realm>",
                  "roles": [
                    "<role-1>",
                    "<role-2>",
                    ...
                  ],
                  "attributes": {
                    "<attr-name-1>": "<attr-value-1>",
                    "<attr-name-2>": "<attr-value-2>",
                    ...
                  }
                },
                "verb": "<retrieve_or_archive>",
                "url": "<download_url_if_ready>",
                "md5": null,
                "collection": "<collection-name>",
                "status": "<status-name>",
                "user_message": "Success",
                "user_request": "<request string>",
                "content_length": null
              },
              {
                "id": "<request-id-2>",
                ...
              },
              ...
            ]
          }

Revoke a request
----------------

Protocol for revoking an active user request.

**Step 1**: DELETE on <POLLING_URL>.

.. list-table::
   :header-rows: 1

   * - Method
     - URL
     - Headers
     - Body
     - Expected codes
     - Response content
   * - DELETE
     - <POLLING_URL>
     - .. code:: bash

          Authorization: <string>
          Host: polytope.ecmwf.int
       | :gray:`For valid authorization headers see authentication and authorization.`
       | :gray:`Host is usually set automatically by your HTTP client.`
     - `-`
     - OK 200
     - | Headers:
       .. code:: bash

          Content-Type: application/json
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            "message": "<info>"
          }

Errors
------

.. list-table::
   :header-rows: 1

   * - Code
     - Reason
     - Response
   * - | 4xx
       | 5xx
     - |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp| |nbsp|
     - | Headers:
       .. code:: bash

          Content-Type: application/json
       | Body: a JSON object (utf8)
       .. code:: bash

          {
            'message': <user-friendly string describing the error>,
            'details': <more information about the error>           # Not always present
          }
   * - 400 (Bad Request)
     - Usually invalid syntax or missing parameters.
     - 
   * - 401 (Unauthorized Request)
     - An Authorization header was not provided or was incorrect.
     - | As above, but with an additional header:
       .. code:: bash

          WWW-Authenticate: <auth_type> realm='some-realm',info='<a short description/hint to the user on how to authenticate>'
   * - 403 (Forbidden Request)
     - Your credentials may be correct but you do not have permission to access the requested resource.
     - 
   * - 404 (Not Found)
     - The requested resource was not found. This often occurs when trying to get the status of a request which has since been deleted; or the URL is somehow malformed. 404 may also mean the request exists but you don't have permission.
     - 
   * - 410 (Gone)
     - May be returned when polling for a request which was previously processed, but has now been cleaned up and removed.
     - 
   * - 500 (Server Error)
     - An unhandled error has occurred on the server. Please report this.
     - 
   * - 501 (Not Implemented)
     - You called an endpoint which is not currently implemented. This may be because the server you are talking to is not configured for certain functionality.
     - 

