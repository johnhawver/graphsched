# GraphSched

A Kubernetes scheduler plugin that makes pod placement topology-aware.

**Status:** Work in progress

## What it does

GraphSched runs alongside the default Kubernetes scheduler as a custom Score plugin. It infers pod communication relationships from Service selectors and environment variable signals, builds a live dependency graph, and scores nodes using a topology-aware heuristic:
```
score = 0.7 × co-location fraction + 0.3 × resource balance
```

Pods that communicate frequently are co-located on the same node or rack, reducing cross-node RPC latency — without requiring manual affinity annotations.

## Why

The default scheduler's ~20 Score plugins are resource-aware but topology-blind. They have no concept of which pods communicate at runtime unless a developer explicitly annotates pod affinity rules. GraphSched infers that topology automatically.

## Stack

Go · Kubernetes scheduler framework · Prometheus · Helm · GitHub Actions
