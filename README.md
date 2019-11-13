# OFD simulation 

Dependencies:
* Docker (tested on Mac)
* jq

The following data is in Postgres: https://github.com/debezium/docker-images/blob/master/examples/postgres/0.6/inventory.sql

* Start all services
    * `make up`    
* Register Postgres with Connect
    * `make register`
* Make a database "ofd_simulation_destination_db" and make customer table in it same as source databse
* Consume messages and run wordcount with Beam
    * `make consumer`
* Stop all services
    * `make down`
* Clean up
    * `make clean`
