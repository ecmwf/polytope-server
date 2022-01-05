.. _federation:

Federation
==========

Federation is the connection of one *polytope-server* to another. There are a few key pieces of configuration required to make this work.

Server One
~~~~~~~~~~

.. code:: yaml

   ...
   # The federation section defines credentials for server two to access server one
   federation:
      example_federation:
         secret: EXAMPLE_SECRET # suggestion: generate a UUID to put here
         allowed_realms: [ example_realm_name ]

   # Define a collection which we will expose via server two
   collections:
      example_collection_server_one:
         roles:
            example_realm_name: [ default ]
         datasources:
            ...

Server Two
~~~~~~~~~~

.. code:: yaml

   ...
   # Add a datasource of type "polytope" with the details of server one, including the secret you generated
   datasources:
      ...
      server1-polytope:
         type: polytope
         url: https://server1.polytope.example.com
         port: 443
         secret: EXAMPLE_SECRET
         api_version: v1
   
   # Create a collection which will use that datasource. Specify the collection on server one which will be used by server two.
   collections:
      example_collection_server_two:
         roles:
            example_realm_name: [ default ]
         limits:
            total: 15
            per-user: 6
         datasources:
            - name: server1-polytope
              collection: example_collection_server_one

Now, when a request is made to server two, using the collection *example_collection_server_two*, it will be forwarded to server one. The result will be sent back via server two to the user, so it is transparent to the user. Server one trusts that server two authenticated the user so will not re-authenticate using the authenticators defined on server one, but it will re-authorize the user by visiting the authorizers. Note that this means any attributes attached by server two's authenticators will not be forwarded.

To enable federation in the other direction, simply add the above configuration in reverse for a different collection. Be careful not to create a circular dependency by forwarding requests to each other *ad infinitum*!