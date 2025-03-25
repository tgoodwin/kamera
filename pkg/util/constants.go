package util

// the "controller name" we use when intercepting garbage collection events
// as part of sleeve webhook request validation.
const GarbageCollectorName = "sleeve:garbage-collector"

// the name controllers can adopt via rest.ImpersonationConfig
// when we are trying to isolate them from other controllers in the
// system that subscribe to the same resources.
// (we use this when re-implementing core controllers in controller-runtime)
const SleeveControllerUsername = "sleeve:controller-user"

// the name we use for API server purge events
// when data is actually removed after being marked for deletion.
const APIServerPurgeName = "sleeve:api-server-purge"
