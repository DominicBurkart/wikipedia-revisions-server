This project serves wikipedia revision differences from a given time 
period, taking an http request with a start datetime and end datetime, 
and sending the revisions via a brotli-compressed stream. The response 
data format is a list of lists of changed text fragments, each fragment 
at least one sentence long.

Build the project:
```shell
docker build -t wikipedia-revisions-server .
```

Run (specifying working & storage directories):
```shell
docker run -v /Volumes/doggo:/working_dir -v /Volumes/burkart-6tb/wiki_revisions:/storage_dir wikipedia-revisions-server -d 20200601
```

If the revisions are already downloaded in the correct format, just 
point to them:
```shell
docker run -v /Volumes/burkart-6tb/wiki_revisions:/storage_dir wikipedia-revisions-server
```