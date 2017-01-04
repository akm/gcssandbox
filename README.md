# gcssandbox

## How to build

```bash
$ gom build
```

## How to run

```bash
$ gcloud beta emulators datastore start
(snip)
[datastore]
[datastore]   export DATASTORE_EMULATOR_HOST=localhost:8649
[datastore]
(snip)
```

```bash
$ export BUCKET=<bucket name>
$ export DATASTORE_EMULATOR_HOST=localhost:8649
$ ./gcssandbox
```
