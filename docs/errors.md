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
| `errors` | Present for `400`, `409` and `422`: one entry per problem, with `field` (the request field, header or path parameter, empty for the body as a whole), a stable `code` and a `message` for humans. At most 50 entries are listed; `detail` gives the total. No `message` contains the submitted value. An entry with the code `duplicate` also carries `existing_id`. |
| `current_status` | Only on a `409` of cancel and redrive: the state the notification is in. |
| `expires_at` | Only on the `409` of a redrive with the code `expires_at_not_allowed`: the stored `expires_at`, in UTC, which is still in the future and is kept. |
| `rejected` | Only on the `422` of `POST /v1/notifications/batch` when every item was rejected: one entry per item with its `index` and its `errors`, never truncated. |

## `malformed-request`

**400 Bad Request.** The request cannot be read: nothing in it was checked further.

| Code | Field | When |
|---|---|---|
| `empty_body` | (empty) | The body is empty or only whitespace. |
| `malformed_json` | (empty) | The body is not valid JSON or not valid UTF-8, or has data after the JSON value. |
| `not_an_object` | (empty) | The body is valid JSON but not an object (an array, a string, a number, `null`). |
| `invalid_uuid` | `id` | The path parameter is not a UUID. |
| `invalid_parameter` | `version` | The `version` of `GET /v1/templates/{name}/versions/{version}` is not a positive integer (`abc`, `0`, `-1`, `1.5`, or too large to be one). |
| `unexpected_body` | (empty) | A cancel request (`POST /v1/notifications/{id}/cancel`, `POST /v1/batches/{id}/cancel`) or a template activation or deactivation (`POST /v1/templates/{name}/activate`, `POST /v1/templates/{name}/deactivate`) has a body. These take none. |
| `unknown_parameter` | the parameter | A query parameter the operation does not define. Only `GET /v1/notifications` defines query parameters; every other operation under `/v1/` rejects any. Rejected rather than ignored, so a misspelled filter never returns an unfiltered list. |
| `duplicate_parameter` | the parameter | A query parameter of the listing is given more than once. Every filter takes a single value. |
| `invalid_parameter` | the parameter (empty for the query string as a whole) | The value is not valid: not one of the allowed values of `status`, `channel` or `priority`, `batch_id` not a UUID, `created_from` or `created_to` not RFC 3339 with an offset, `limit` not an integer from 1 to 200, an empty value, or a query string that is not well formed. |
| `invalid_cursor` | `cursor` | The cursor is not a `next_cursor` this API returned. |
| `invalid_idempotency_key` | `Idempotency-Key` | The header is empty, longer than 255 bytes, contains a byte outside U+0021 to U+007E (a space included), or is sent more than once. |

## `not-found`

**404 Not Found.** No notification, batch or template matches the request (an unknown id, a template name that does not exist or cannot exist because it does not match `^[a-z0-9][a-z0-9_-]{0,63}$`, a template version that does not exist), or no resource exists at this path. A path that is not in canonical form (`//healthz`, a `..` segment) is answered the same way instead of being redirected.

## `method-not-allowed`

**405 Method Not Allowed.** The path exists but does not support the method. The `Allow` header lists the methods it supports.

## `payload-too-large`

**413 Content Too Large.** The request body is larger than `MAX_BODY_BYTES` (5 MB by default).

## `unsupported-media-type`

**415 Unsupported Media Type.** A request with a body must be sent with `Content-Type: application/json` (parameters such as `charset=utf-8` are allowed).

## `validation`

**422 Unprocessable Content.** The body is a JSON object, but fields are invalid. Every invalid field is reported in one answer: fields of the request in the order `channel`, `recipient`, `content`, `priority`, `dedupe_key`, then unknown fields in the order they appear.

`recipient` and `content` are checked in two steps. First the rules every channel shares (type, empty, length, U+0000); then, only when that step passed and `channel` is valid, the rules of the channel. A field therefore has at most one error, and a missing or invalid `channel` gives no channel-specific error.

| Code | When |
|---|---|
| `required` | A required field (`channel`, `recipient`, `content`; `notifications` in a batch; `name`, `channel`, `body` of a template) is missing. |
| `invalid_type` | The field has the wrong JSON type; `null` is a wrong type for every field. `variables` and `optional_variables` of a template must be arrays of strings; the error is on the list's field whatever element is wrong. In a batch, `notifications` must be an array, and an item that is not a JSON object is rejected with this code and an empty `field`. |
| `invalid_value` | The value is not one of the allowed values (`channel`: `sms`, `email`, `push`; `priority`: `high`, `normal`, `low`), or `name` of a template does not match `^[a-z0-9][a-z0-9_-]{0,63}$`, or `expires_at` of a redrive is not an RFC 3339 date-time with an offset. |
| `empty` | `recipient`, `content`, `dedupe_key` or `body` of a template is an empty string, or `notifications` is an empty array. |
| `too_long` | `recipient` is longer than 4,096 characters, or `dedupe_key` longer than 255 characters; or `body` of a template is longer than 65,536 bytes (or renders to more than 524,288 bytes); or `content` is longer than 102,400 bytes of UTF-8 for `email`, or 4,096 bytes for `push`. |
| `invalid_e164` | `recipient` of an `sms` is not a possible E.164 number (`PHONE_VALIDATION=possible`, the default: length and structure for the country code) or not a valid one (`strict`: the national numbering plan). The number must already be in E.164 form, such as `+905551234567`; `05551234567` is not normalised but refused. |
| `invalid_email` | `recipient` of an `email` is not a single address, or carries a display name (`Ali <ali@example.com>`): the parsed address must equal the input. At most 254 characters. |
| `invalid_token` | `recipient` of a `push` is not a device token: 1 to 4,096 characters without control characters. |
| `too_many_segments` | `content` of an `sms` needs more than 10 segments. The encoding is GSM-7 (160 septets in one segment, 153 per segment when concatenated; the extension characters `€`, `[`, `]`, `{`, `}`, `^`, `~`, `\|` and `\` cost two) unless one character is outside the GSM-7 alphabet (for example `ş`, `ğ`, `ı`), which puts the whole message into UCS-2 (70 UTF-16 code units, 67 when concatenated). An escape sequence or a surrogate pair is never split across segments. |
| `invalid_character` | `recipient`, `content`, `dedupe_key` or `body` of a template contains the character U+0000, which cannot be stored. |
| `too_many_items` | `notifications` has more than 1,000 items. No item is checked then. |
| `duplicate_in_batch` | `dedupe_key` of a batch item: another valid item of the same batch, earlier in the array, has the same `dedupe_key`. The first valid item stays; an invalid item never claims its key. |
| `too_soon` | `expires_at` of a redrive is closer than 30 seconds (`EXPIRES_AT_MIN_LEAD`). |
| `unused_variable` | `variables` of a template: a declared variable (`variables` or `optional_variables`) is not used by the body. The message names the variable. |
| `invalid_variable_name` | `variables` of a template: a variable name does not match `^[A-Za-z_][A-Za-z0-9_]{0,63}$` (ASCII only), in the declaration or in the body. |
| `duplicate_variable` | `variables` of a template: a name is declared twice, in one list or across both. |
| `too_many_variables` | `variables` of a template: more than 50 variables are declared, both lists together. |
| `template_syntax` | `body` of a template does not parse. The message gives the line when it is known and never the parser's text. |
| `construct_not_allowed` | `body` of a template uses a construct outside the allow-list (`range`, `with`, `template`, `block`, variables, chained fields, the bare dot, and so on). The message names the construct and the line. |
| `function_not_allowed` | `body` of a template calls a function other than `default` and `truncate`; every builtin function of Go's template language is refused. The message names the function and the line. |
| `multiple_templates` | `body` of a template defines more than one template (`{{ define }}`). |
| `invalid_call` | `body` of a template calls `default` or `truncate` with the wrong arguments. |
| `undeclared_variable` | `body` of a template uses a variable that is not declared in `variables` or `optional_variables`. |
| `unsafe_html` | `body` of an e-mail template is refused by the HTML escaper, for example because it ends inside a tag, an attribute, a comment or a `<script>`. An e-mail body must be HTML that ends in plain text context. |
| `unknown_field` | The API does not define this field. It is rejected rather than ignored, so a field of a later version (for example `scheduled_at`) never leads to a behaviour the client did not ask for. |
| `duplicate_field` | The field appears more than once in the object. |

### Template answers

`POST /v1/templates` lists its fields in the order `name`, `channel`, `body`, `variables`, `optional_variables`, then unknown fields; `POST /v1/templates/{name}/versions` has no `name` and no `channel`. Only when every field passed this check is the body compiled for the channel, and then the first problem the engine finds is the one error of the answer: its code is the code of the problem (`template_syntax`, `construct_not_allowed`, ...), its field is `variables` for `unused_variable`, `invalid_variable_name`, `duplicate_variable` and `too_many_variables` and `body` for every other problem. A message names the construct, function or variable and the line, never other text of the body.

### Batch answers

`POST /v1/notifications/batch` checks the top level first (`notifications` present once, an array of 1 to 1,000 items, no other field); any error there is one `422` and no item is checked. Otherwise every item is checked on its own, and its errors are listed with its position in the array. The answer is `202` with `accepted` and `rejected` when at least one item is stored, and `422` with `rejected` (and no `errors`) when none is.

## `duplicate`

**409 Conflict.** A notification with this `dedupe_key` already exists. The entry of `errors` has the field `dedupe_key`, the code `duplicate` and `existing_id`: the id of the notification that holds the key. `existing_id` is absent when that notification released the key in the same instant (it was cancelled before any attempt); the request can then be sent again.

## `template-exists`

**409 Conflict.** `POST /v1/templates` named a template that already exists. The entry of `errors` has the field `name` and the code `template_exists`. New content for an existing name goes through `POST /v1/templates/{name}/versions`.

## `conflict`

**409 Conflict.** The notification's state does not allow the operation. `current_status` is the state it is in.

| Code | Field | When |
|---|---|---|
| `not_cancellable` | `status` | Cancel: the notification is `processing` (a worker holds it), `sent` or `failed`. A notification that is already `cancelled` is not an error: the cancel answers `200` again. |
| `not_failed` | `status` | Redrive: the notification is not `failed`. |
| `expires_at_required` | `expires_at` | Redrive: the stored `expires_at` has passed, so the request must give a new one. |
| `expires_at_not_allowed` | `expires_at` | Redrive: the stored `expires_at` is still in the future and is kept, so the request must not give one; the problem document carries the stored value as `expires_at`. |

A `409` without `errors` means that the notification changed state while the request ran; the request can be repeated.

## `idempotency-key-reused`

**422 Unprocessable Content.** This `Idempotency-Key` was already used with a different request (another method, path or body). Send a new key for a new request; a retry of the same request returns the stored answer. The entry of `errors` has the field `Idempotency-Key` and the code `idempotency_key_reused`.

## `internal`

**500 Internal Server Error.** An unexpected error. The answer does not describe the cause; the log line of the request, found by its `correlation_id`, does.

## `unavailable`

**503 Service Unavailable.** Postgres cannot be reached or does not answer in time. The request did not take effect and can be retried. `GET /readyz` answers `503` in the same situation; there the document also carries `checks`, with the name, status and duration of each dependency check.
