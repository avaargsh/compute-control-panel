# Disposable PD JobSet provider API gate

Test-only extraction from #115. The same kind cluster runs the actual JobSet adapter with its mounted, GET/CREATE-only staging ServiceAccount and CA. The fixture deliberately uses an in-memory UID binding store: reconstructing the adapter in the same process is not a restart or durable evidence. Assert one inert JobSet, same UID on replay, and UNKNOWN/OperationInProgress with no binding. The `real_pd_jobset_writer` receipt must explicitly set durable_uid_pin_verified, physical_gpu_verified, driver_prepared, allocation_ready, released, and cost_final to false. No runtime mounting, activation, physical release, G04/G05, or finance authorization.

The next independent gate must call StageInertPDJobSet through the real PostgreSQL source fence and restricted durable binding login. A separate-process SIGKILL/inspect-first experiment remains required.
