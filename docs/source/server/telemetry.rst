.. _telemetry:

Telemetry
=========

If the ``telemetry`` section is populated in the Polytope's configuration file, a telemetry microservice will be deployed to collect various metrics on system activity and status, and expose them in JSON format via ``/telemetry/v1``.

Note that telemetry is not exposed by the ingresses (if deployed) in kubernetes or docker. This means telemetry will not be accessible using the public service URL. It will only accessible at the local ``host:port/telemetry/v1``. Also, this means telemetry won't be reachable through HTTPs if enabled.

Also note that, since telemetry is not exposed by the ingress, no authentication credentials are required for HTTP conversations with it.

The different telemetry endpoints exposed by telemetry can be listed by sending a HTTP GET request to the root telemetry endpoint, e.g.:

.. code:: bash

   curl -X GET http://<SERVICE_HOST>:32012/telemetry/v1 | python3 -m json.tool 
   # {
   #     "message": [
   #         "test",
   #         "summary",
   #         "all",
   #         "requests",
   #         "workers"
   #     ]
   # }

A GET request to ``/summary`` returns metrics on general system status, such as database use and number of entries, or cache hits/misses. ``/requests`` returns information on active requests in the system, such as request ID, submitting user, request description and status. ``/requests?id=<REQ_ID>`` returns information on a specific request, with a trace of all microservices that dealt with that request. ``/workers`` returns metrics on the various workers deployed as part of Polytope, including the number of requests they've served, the failures encountered, time spent processing, etc.