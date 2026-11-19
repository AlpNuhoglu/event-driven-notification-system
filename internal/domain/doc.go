// Package domain holds the pure core rules of the notification system: the
// states and their transitions, the retry decision and the typed values
// (channel, priority, recipient), and the template engine: the allow-list
// that decides which template sources are accepted, and their rendering in
// memory. It performs no I/O.
package domain
