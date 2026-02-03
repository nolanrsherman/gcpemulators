# gcpemulators

A collection of GCP emulators designed to be native to Go and easy to integrate into your test environment.

# About

This project was born out of the need for a testing environment that closely mirrors production GCP services and enabled testing and developing offline. When testing applications that rely on GCP services, it's crucial to have emulators that behave as similarly as possible to the real services. This ensures that your tests catch issues before they reach production and that your development workflow isn't blocked by external dependencies.

This is a new project, and I'd welcome contributions! Whether you want to add support for additional GCP services, improve existing emulators, fix bugs, or enhance documentation, your help is appreciated.

# How to Contribute

Since this project is new, there is not an established structure here yet. Just open up a Pull Request about a service you want to emulate or and bug/features you want to address and lets chat.


# Supported Services

| Service       | Status      |   # fn   | gRPC support | HTTP support |
|--------------|--------------|----------|--------------|--------------|
| apigateway   | planned      |    0     | TBD          | TBD          |
| appengine    | planned      |    0     | TBD          | TBD          |
| auth         | planned      |    0     | TBD          | TBD          |
| bigquery     | planned      |    0     | TBD          | TBD          |
| bigtable     | planned      |    0     | TBD          | TBD          |
| cloudsqlconn | planned      |    0     | TBD          | TBD          |
| cloudtasks   | in progress  |  13/16   | TBD          | TBD          |
| filestore    | planned      |    0     | TBD          | TBD          |
| firestore    | planned      |    0     | TBD          | TBD          |
| functions    | planned      |    0     | TBD          | TBD          |
| pubsub       | planned      |    0     | TBD          | TBD          |
| scheduler    | planned      |    0     | TBD          | TBD          |
| secretmanager| planned      |    0     | TBD          | TBD          |
| storage      | in progress  |   4/29   | TBD          | TBD          |
| workflows    | planned      |    0     | TBD          | TBD          |