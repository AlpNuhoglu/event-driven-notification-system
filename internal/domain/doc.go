// Package domain holds the pure core rules of the notification system: the
// states and their transitions, the retry decision and the typed values
// (channel, priority, recipient). It performs no I/O and imports only the
// standard library and github.com/google/uuid.
package domain
