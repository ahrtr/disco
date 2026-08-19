// Package fencing defines the Token type and helpers for injecting and
// extracting fencing tokens across HTTP and gRPC transports.
//
// HTTP convention: X-Fencing-Token request header.
// gRPC convention: x-fencing-token metadata key.
package fencing
