// Package provider is the HTTP client that hands a notification to the
// external webhook provider. It sends exactly one request per attempt and
// reports what happened; it does not classify outcomes (the worker does).
package provider
