# API errors

Every non-2xx answer of the API is an RFC 9457 problem with `Content-Type: application/problem+json` ([ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md) §9). This holds for the answers of the router itself as well: an unknown path, a method a path does not support.

```json
{
  "type": "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#validation",
  "title": "Validation failed",
  "status": 422,
  "detail": "2 field(s) are invalid.",
  "correlation_id": "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4",
  "errors": [
    { "field": "recipient", "code": "empty", "message": "recipient must not be empty" },
    { "field": "scheduled_at", "code": "unknown_field", "message": "this field is not defined by the API" }
  ]
}
```

| Member | Meaning |
|---|---|
| `type` | This page, with the anchor of the problem type. Stable. |
| `title` | A short summary of the problem type. |
| `status` | The HTTP status. |
| `detail` | An explanation of this occurrence. It never contains a recipient, message content or an internal cause. |
| `correlation_id` | The request's correlation id, also returned in the `X-Request-ID` header. Quote it when reporting a problem. |
| `errors` | Present for `400` and `422`: one entry per problem, with `field` (the request field or path parameter, empty for the body as a whole), a stable `code` and a `message` for humans. At most 50 entries are listed; `detail` gives the total. No `message` contains the submitted value. |

## `malformed-request`

**400 Bad Request.** The request cannot be read: nothing in it was checked further.

| Code | Field | When |
|---|---|---|
| `empty_body` | (empty) | The body is empty or only whitespace. |
| `malformed_json` | (empty) | The body is not valid JSON or not valid UTF-8, or has data after the JSON value. |
| `not_an_object` | (empty) | The body is valid JSON but not an object (an array, a string, a number, `null`). |
| `invalid_uuid` | `id` | The path parameter is not a UUID. |

## `not-found`

**404 Not Found.** No notification has this id, or no resource exists at this path. A path that is not in canonical form (`//healthz`, a `..` segment) is answered the same way instead of being redirected.

## `method-not-allowed`

**405 Method Not Allowed.** The path exists but does not support the method. The `Allow` header lists the methods it supports.

## `payload-too-large`

**413 Content Too Large.** The request body is larger than `MAX_BODY_BYTES` (5 MB by default).

## `unsupported-media-type`

**415 Unsupported Media Type.** A request with a body must be sent with `Content-Type: application/json` (parameters such as `charset=utf-8` are allowed).

## `validation`

**422 Unprocessable Content.** The body is a JSON object, but fields are invalid. Every invalid field is reported in one answer: fields of the request in the order `channel`, `recipient`, `content`, `priority`, then unknown fields in the order they appear.

| Code | When |
|---|---|
| `required` | A required field (`channel`, `recipient`, `content`) is missing. |
| `invalid_type` | The field has the wrong JSON type; `null` is a wrong type for every field. |
| `invalid_value` | The value is not one of the allowed values (`channel`: `sms`, `email`, `push`; `priority`: `high`, `normal`, `low`). |
| `empty` | `recipient` or `content` is an empty string. |
| `too_long` | `recipient` is longer than 512 characters. |
| `invalid_character` | `recipient` or `content` contains the character U+0000, which cannot be stored. |
| `unknown_field` | The API does not define this field. It is rejected rather than ignored, so a field of a later version (for example `scheduled_at`) never leads to a behaviour the client did not ask for. |
| `duplicate_field` | The field appears more than once in the object. |

## `internal`

**500 Internal Server Error.** An unexpected error. The answer does not describe the cause; the log line of the request, found by its `correlation_id`, does.

## `unavailable`

**503 Service Unavailable.** Postgres cannot be reached or does not answer in time. The request did not take effect and can be retried. `GET /readyz` answers `503` in the same situation; there the document also carries `checks`, with the name, status and duration of each dependency check.
