<!--
SPDX-FileCopyrightText: 2026 ECMWF

SPDX-License-Identifier: Apache-2.0
-->

# MARS callback relay

The MARS worker exposes one always-on TCP relay for DHS callbacks. Its per-Pod
NodePort Service targets the relay listener, which defaults to port `18100`.
Configure it with `--mars-callback-relay-port` or `MARS_CALLBACK_RELAY_PORT`.

For each retrieval the worker:

1. selects a fresh, currently-free internal TCP port;
2. registers that port as a new relay generation;
3. sets `MARS_DHS_LOCALPORT` while holding the serialized MARS environment lock;
4. runs the C++ MARS call; and
5. cancels the generation when the retrieval and stream close.

An inbound callback snapshots the active generation. If the C++ callback
listener has not bound yet, the relay retains that connection and retries
`127.0.0.1:<internal-port>`. Once connected, Tokio forwards both directions
with backpressure. All connections from the same retrieval use the same target.
They are cancelled when that generation ends and cannot be retargeted to a
later retrieval.

The relay binds before the NodePort Service is created and before work polling
starts. Broker worker concurrency may be greater than one. The process-wide
environment lock serializes port selection, generation lifetime, environment
mutation, and the complete blocking C++ retrieval so only one MARS call is
active at a time. Other claimed jobs queue behind that lock.

## Deployment values

The Pod must allow inbound TCP on the configured relay port and pass the usual
`POD_NAME`, `POD_UID`, `POD_NAMESPACE`, and `K8S_NODE_NAME` environment values.
The worker creates and reconciles its own NodePort Service target. The
advertised callback remains the Kubernetes node name and allocated NodePort;
internal generation ports are never exposed through Kubernetes.

See [`../examples/mars-worker-callback-relay.yaml`](../examples/mars-worker-callback-relay.yaml)
for the relevant container fragment. The image does not require an `EXPOSE`
instruction for this to work.
