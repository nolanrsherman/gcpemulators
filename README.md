# gcpemulators

A collection of GCP emulators designed to be native to Go and easy to integrate into your test environment.

# About

This project was born out of the need for a testing environment that closely mirrors production GCP services and enabled testing and developing offline. When testing applications that rely on GCP services, it's crucial to have emulators that behave as similarly as possible to the real services. This ensures that your tests catch issues before they reach production and that your development workflow isn't blocked by external dependencies.

This is a new project, and I'd welcome contributions! Whether you want to add support for additional GCP services, improve existing emulators, fix bugs, or enhance documentation, your help is appreciated.

# Challenges

One great thing about GCP services is that most of the APIs have protocol buffers. This makes it easy to get started with handling the requests from the client libraries. But, sometimes like in the case of storage, the client library ("cloud.google.com/go/storage") uses protobufs that are not in the public protobuf packages ("cloud.google.com/go/storage/internal/apiv2/storagepb") but instead uses an internal one which is different than the published public one for the same service. 

This is an issue when compiling since both packages, if used at the same time, will register conflicting protobuf namespaces (panics). One solution im experimenting with now is just offering this emulator as a docker container, and some helpful library commands for spinning up new instances. This way, the conflicting namespaces issue is avoided since the emulator is compiled/running on a different process/binary all together.

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
| cloudtasks   | in progress  |  13/16   |  ✅          | TBD          |
| filestore    | planned      |    0     | TBD          | TBD          |
| firestore    | planned      |    0     | TBD          | TBD          |
| functions    | planned      |    0     | TBD          | TBD          |
| pubsub       | planned      |    0     | TBD          | TBD          |
| scheduler    | planned      |    0     | TBD          | TBD          |
| secretmanager| planned      |    0     | TBD          | TBD          |
| storage      | in progress  |  10/24   |  ✅          | TBD          |
| workflows    | planned      |    0     | TBD          | TBD          |