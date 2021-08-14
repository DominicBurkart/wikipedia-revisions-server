```
  _      ___ __    _   ___           _     _                 ____
 | | /| / (_) /__ (_) / _ \___ _  __(_)__ (_)__  ___  ___   / __/__ _____  _____ ____
 | |/ |/ / /  '_// / / , _/ -_) |/ / (_-</ / _ \/ _ \(_-<  _\ \/ -_) __/ |/ / -_) __/
 |__/|__/_/_/\_\/_/ /_/|_|\__/|___/_/___/_/\___/_//_/___/ /___/\__/_/  |___/\__/_/
```


[![status](https://github.com/DominicBurkart/wikipedia-revisions-server/workflows/Docker%20Image%20CI/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions-server/actions?query=is%3Acompleted+branch%3Amaster+workflow%3A"Docker+Image+CI") [![status](https://github.com/DominicBurkart/wikipedia-revisions-server/workflows/rust%20linters/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions-server/actions?query=is%3Acompleted+branch%3Amaster+workflow%3A"rust+linters")

This project serves wikipedia revision differences from a given time
period, taking an http request with a start datetime and end datetime,
and sending the revisions via a brotli-compressed stream. In the response
stream, each line is a JSON-encoded revision.

documentation coming soon 🥧⏲️

Build the project:
```shell
docker build -t wikipedia-revisions-server .
```

Run (specifying working & storage directories, plus dump date):
```shell
docker run -it -v /local/path:/fast_dir -v /other/local/path:/big_dir wikipedia-revisions-server -d 20200601
```

If the data and index files have already been built, you can start the server without having to rebuild:
```shell
docker run -it -v /local/path:/fast_dir -v /other/local/path:/big_dir wikipedia-revisions-server
```

To find a valid date (-d param), go to the [wiki archives](https://dumps.wikimedia.org/enwiki/) and find a date with available .xml.bz2 files to download for "All pages with complete page edit history"

See the python [wikipedia revisions](https://github.com/dominicburkart/wikipedia-revisions) repo for different download targets & schemes than those available here.

### Distribution

While building the datastore, each revision of each wikipedia page must be individually compressed.
For me, this was the limiting factor while building the data files and indices. To optionally speed
up this portion of the task, a private Kubernetes cluster can be leveraged to distribute the
compression tasks, so that the primary program is essentially only responsible for reading revisions,
sending them to the cluster to be compressed, receiving the compressed revisions, and then writing
the compressed revisions and their indices to the relevant files.

Additional requirements for distribution:

- All [Turbolift requirements](https://dominic.computer/turbolift#kubernetes-requirements)
  - to access docker inside a docker container, we expose the docker socket as a volume (`-v /var/run/docker.sock:/var/run/docker.sock`).
  Example: `docker run -it -v /var/run/docker.sock:/var/run/docker.sock -v /local/path:/fast_dir -v /other/local/path:/big_dir wikipedia-revisions-server`
- The compression image is published to the user's public Docker hub as a public image. The user
must initialize the docker hub repository manually and log into the docker CLI as described
in the [docker docs](https://docs.docker.com/docker-hub), so that the program may push the image.

The program's default is to allow up to 32 concurrent replicas. Each replica is capable of using
multiple cores while processing concurrent requests. The practical limitation on usable cores
per replica in the current implementation is roughly
`min(number of cores on main program host, 200) * 32`, though the program was intended to leverage
small-machine clusters and has not be tested with large nodes.

Thanks to [JetBrains](https://www.jetbrains.com/?from=WikipediaRevisionsServer) for providing an open source license to their IDEs for developing this project!
