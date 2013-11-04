atlas-deer
==========

This contains all the projects relating to the 4.0 API release of Atlas, a.k.a `deer`. It replaces the deer branches on the other existing atlasapi/* repositories.

The repository is divided in 5 projects:

* atlas-api: the Atlas API 4.0 web-app containing HTTP controllers, query executors and model serializers.
* atlas-processing: the back-end web-app for running scheduled tasks.
* atlas-core: defines the model and interfaces on which atlas-api and atlas-processing rely and their common and base implementations.
* atlas-cassandra: Cassandra-based implementations of the core persistence interfaces.
* atlas-elasticsearch: elasticsearch-based implementations of the core index interfaces.
