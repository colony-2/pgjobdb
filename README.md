# pgjobdb

`pgjobdb` is the Postgres scheduler and optional runtime adapter for JobDB.
Applications that choose another runtime do not need to import this module.

This repository is under construction. Its first deployment requires a new,
empty Postgres database. It does not migrate or adopt existing `pgwf` data.

Run the current checks with:

```sh
go test ./...
```
