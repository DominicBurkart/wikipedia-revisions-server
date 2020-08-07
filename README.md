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
docker run -it -v /Volumes/doggo:/fast_dir -v /Volumes/biggo/wiki_revisions:/big_dir wikipedia-revisions-server -d 20200601
```

If the data and index files have already been built, you can start the server without having to rebuild:
```shell
docker run -it -v /Volumes/doggo:/fast_dir -v /Volumes/biggo/wiki_revisions:/big_dir wikipedia-revisions-server
```

To find a valid date (-d param), go to the [wiki archives](https://dumps.wikimedia.org/enwiki/) and find a date with available .xml.bz2 files to download for "All pages with complete page edit history"

See the python [wikipedia revisions](https://github.com/dominicburkart/wikipedia-revisions) repo for different download targets & schemes than those available here.
