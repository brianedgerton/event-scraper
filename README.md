# event-scraper

This is a utility for exporting actors and their related events out of Riak. It will also import a generated archive of actors and events back into Riak.

## Usage

To export:

```sh
$ ./scraper export --actorBucket actor_stuff --eventBucket event_stuff --keyFile myKeys.txt
```

This `keyFile` should be a text file containing 1 key per line. These keys should represent the actors in which you are interested.

This command with produce an `export.tar.gz` file that contains the data from those buckets.

```sh
Usage: scraper-export [options]

Options:

  -h, --help                       output usage information
  -V, --version                    output the version number
  -H, --host [host]                Riak Host (default: localhost)
  -p, --port [port]                Riak Port (default: 8089)
  -d, --dir [directoryName]        Directory in which to place exported files (default: ./export)
  -k, --keyFile [keyFileName]      Path to file from which keys can be read
  -a, --actorBucket [actorBucket]  Bucket which contains actors
  -e, --eventBucket [eventBucket]  Bucket which contains the actors' events
```

To import:

```sh
$ ./scraper export --actorBucket imported_actors --eventBucket imported_events --file export.tar.gz
```

This will extract the events exported by the above command and put them into the specified buckets while preserving indexes.

```sh
Usage: scraper-import [options]

Options:

  -h, --help                       output usage information
  -V, --version                    output the version number
  -H, --host [host]                Riak Host (default: localhost)
  -p, --port [port]                Riak Port
  -f, --file [fileName]            Path to archive file from which to import
  -a, --actorBucket [actorBucket]  Bucket in which to import actors
  -e, --eventBucket [eventBucket]  Bucket in which to import the actors' events
```