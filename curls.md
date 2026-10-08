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
