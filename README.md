This project serves wikipedia revision differences from a given time 
period, taking an http request with a start datetime and end datetime, 
and sending the revisions via a brotli-compressed stream.

Build the project:
```shell
docker build -t wikipedia-revisions-server .
```

Run (specifying working & storage directories, plus dump date):
```shell
docker run -it -v /Volumes/doggo:/fast_dir -v /Volumes/biggo/wiki_revisions:/big_dir wikipedia-revisions-server -d 20200601
```

If the data and index files have already been built, you can start the server without having to rebuild:
```shell
docker run -it -v /Volumes/doggo:/fast_dir -v /Volumes/biggo/wiki_revisions:/big_dir wikipedia-revisions-server
```

To find a valid date (-d param), go to the [wiki archives](https://dumps.wikimedia.org/enwiki/) and find a date with available .xml.bz2 files to download for "All pages with complete page edit history"

See the python [wikipedia revisions](https://github.com/dominicburkart/wikipedia-revisions) repo for different download targets & schemes than those available here.
