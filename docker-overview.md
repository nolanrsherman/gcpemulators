# GCP Emulators

Github: https://github.com/nolanrsherman/gcpemulators

# How to use

## Start a Storage Emulator
```sh
docker run -p 6351:6351--rm gcpemulators storage
```

## Start a Cloud Tasks Emulator
```sh
docker run -p 6351:6351--rm gcpemulators cloudtasks
```