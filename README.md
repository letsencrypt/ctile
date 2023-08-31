# CTile

This is a caching proxy for the get-entries endpoint of a CT log, which uses S3
as its backing store. It uses the concept of "tiles" of entries, where each tile
is a fixed size N (e.g. 256 log entries) and the sequence of tiles starts at 0.
Regardless of what `start` and `end` parameters CTile receives for a request, it
will transform those into a tile-sized request to its backend, by rounding down
`start` to the nearest multiple of N and requesting exactly N items from the
backend. If the request is successful, CTile checks that the response contains
exactly N items, re-encodes as gzipped CBOR, and stores the result in S3. It then
returns modified JSON to the user, removing items from the head and tail to ensure
that the first entry actually corresponds to the first entry requested by the user
and that the response includes at most as many entries as requested.

When looking up entries in the cache, CTile also rounds `start` down to the
nearest multiple of N, and requests a single tile from the S3 backend. The CT
protocol allows the server to return fewer results than requested, so CTile does
not attempt to request multiple tiles to fulfil a large request. If a request's
`start` parameter is one less than the end of a tile, CTile will respond with a
single entry. This is similar to how Trillian's [align_getentries
flag](https://github.com/google/certificate-transparency-go/blob/6e118585d9d9757b739353829becec378f47e10b/trillian/ctfe/handlers.go#L50)
works, and is in fact compatible with that flag, so long as CTile's tile size is
less than or equal to Trillian's max_get_entries flag.

When a user requests a range of get-entries near the end of the log, CTile
usually won't be able to get a full tile's worth of entries from the backend,
because the requisite number of entries haven't been sequenced yet. In this
case, CTile does not write anything to the S3 backend and simply passes
through the entries returned from the server (after appropriate tweaks to match
the start and end parameters from the user request).

# How To

You must have an S3 bucket set up, and AWS credentials for a role that has read
and write access to that S3 bucket. CTile uses the AWS Go SDK with the default
credential provider, and so will [pull credential
information](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials)
from environment variables, an AWS config file, or ambient credentials for an
EC2 instance. You'll need to manually specify the AWS region for your S3 bucket
by setting the environment variable AWS_REGION.

You must also know the maximum get-entries size for the log you are mirroring.
If you operate the log, you will know this from your own configs. Otherwise, you
can figure it out by making a get-entries request with `end` much larger than
`start`, and counting the entries. You should set CTile's tile-size to exactly
equal this maximum. It's possible to set a tile-size lower, but only if the log
is not using `align_getentries`.

Example invocation:

```
export AWS_REGION=us-west-2
go run . -log-url https://oak.ct.letsencrypt.org/2023 \
    -tile-size 256 -s3-bucket some-bucket -full-request-timeout 30s -s3-prefix oak2023
```

```
curl 'localhost:8080/ct/v1/get-entries?start=0&end=999999999' -i  | less
```
