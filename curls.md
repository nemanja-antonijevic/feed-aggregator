# curl examples

Assumes the application runs on `localhost:8080` and MongoDB is up (`docker compose up -d`).
Responses below were captured from a real run; ids and timestamps will differ on yours.

```shell
BASE=http://localhost:8080
```

## Create a feed

```shell
curl -i -X POST "$BASE/feeds" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/a.xml","title":"Feed A"}'
```

```text
HTTP/1.1 201 Created
Location: /feeds/6ac78097a074eb4e5b03e88b
Content-Type: application/json

{"id":"6ac78097a074eb4e5b03e88b","url":"https://example.com/a.xml","title":"Feed A","createdAt":"2026-10-08T11:37:59.817Z"}
```

## Duplicate url

Same request again returns `409`:

```shell
curl -i -X POST "$BASE/feeds" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/a.xml","title":"dup"}'
```

```text
HTTP/1.1 409 Conflict
Content-Type: application/json

{"message":"url already exists"}
```

## Invalid body

Both fields missing. Only the first field alphabetically is reported:

```shell
curl -i -X POST "$BASE/feeds" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"message":"title must not be blank"}
```

Not an http or https URL:

```shell
curl -i -X POST "$BASE/feeds" \
  -H 'Content-Type: application/json' \
  -d '{"url":"ftp://x.com/a","title":"t"}'
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"message":"url must be an absolute http or https URL"}
```

## List feeds

Newest first (`createdAt` descending, then `id` descending). Returns `[]` when empty.

```shell
curl -i "$BASE/feeds"
```

```text
HTTP/1.1 200 OK
Content-Type: application/json

[{"id":"6ac78097a074eb4e5b03e88c","url":"https://example.com/b.xml","title":"Feed B","createdAt":"2026-10-08T11:37:59.885Z"},{"id":"6ac78097a074eb4e5b03e88b","url":"https://example.com/a.xml","title":"Feed A","createdAt":"2026-10-08T11:37:59.817Z"}]
```

## Get one feed

```shell
curl -i "$BASE/feeds/6ac78097a074eb4e5b03e88b"
```

```text
HTTP/1.1 200 OK
Content-Type: application/json

{"id":"6ac78097a074eb4e5b03e88b","url":"https://example.com/a.xml","title":"Feed A","createdAt":"2026-10-08T11:37:59.817Z"}
```

Valid id that does not exist:

```shell
curl -i "$BASE/feeds/507f1f77bcf86cd799439099"
```

```text
HTTP/1.1 404 Not Found
Content-Type: application/json

{"message":"feed not found: 507f1f77bcf86cd799439099"}
```

Malformed id:

```shell
curl -i "$BASE/feeds/abc"
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"message":"id must be a valid ObjectId: abc"}
```

## Delete a feed

```shell
curl -i -X DELETE "$BASE/feeds/6ac78097a074eb4e5b03e88b"
```

```text
HTTP/1.1 204 No Content
```

Repeating the same call returns `404`:

```shell
curl -i -X DELETE "$BASE/feeds/6ac78097a074eb4e5b03e88b"
```

```text
HTTP/1.1 404 Not Found
Content-Type: application/json

{"message":"feed not found: 6ac78097a074eb4e5b03e88b"}
```

Malformed id returns `400`:

```shell
curl -i -X DELETE "$BASE/feeds/abc"
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"message":"id must be a valid ObjectId: abc"}
```

## Fetch a feed

Downloads the feed URL, parses RSS or Atom, and stores the entries as items. The request is synchronous; the response says what happened:

```shell
curl -i -X POST "$BASE/feeds/6ac78097a074eb4e5b03e88b/fetch"
```

```text
HTTP/1.1 200 OK
Content-Type: application/json

{"fetched":2,"inserted":2,"duplicates":0}
```

Fetching the same feed again inserts nothing, because `(feedId, guid)` is unique:

```text
HTTP/1.1 200 OK
Content-Type: application/json

{"fetched":2,"inserted":0,"duplicates":2}
```

An Atom feed works the same way:

```text
{"fetched":1,"inserted":1,"duplicates":0}
```

## Fetch errors

The source answered with an HTTP error (any non-2xx status, including 404 and 500):

```text
HTTP/1.1 502 Bad Gateway
Content-Type: application/json

{"message":"feed source returned HTTP 404"}
```

The source did not answer within 3 seconds in total:

```text
HTTP/1.1 504 Gateway Timeout
Content-Type: application/json

{"message":"feed source timed out"}
```

The source returned malformed XML:

```text
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/json

{"message":"feed source returned an invalid feed"}
```

The source returned an HTML page (`Content-Type: text/html`):

```text
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/json

{"message":"feed source did not return an RSS or Atom feed"}
```

Unknown or malformed local feed id:

```text
HTTP/1.1 404 Not Found

{"message":"feed not found: 6ac78097a074eb4e5b03e88b"}
```

```text
HTTP/1.1 400 Bad Request

{"message":"id must be a valid ObjectId: xyz"}
```

Not captured from a real run, but covered by integration tests: a body over 1 MiB and a connection closed mid-body return `502` with `feed source body exceeds 1 MiB` and `feed source request failed`.

## Known framework-level error shape

Malformed JSON and an empty body do not use `{"message"}`; they return Spring's default error body:

```shell
curl -i -X POST "$BASE/feeds" -H 'Content-Type: application/json' -d '{bad'
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"timestamp":"2026-10-08T11:38:03.279Z","path":"/feeds","status":400,"error":"Bad Request","requestId":"3a6eadfb-16"}
```
