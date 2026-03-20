## Day 2 — Scheduler internals + cluster validation

# Watch scheduler logs live
kubectl logs -n kube-system -l component=kube-scheduler -f

# Run a test pod and check which node it landed on
kubectl run watcher-test --image=nginx --restart=Never
kubectl get pod watcher-test -o wide

# Clean up test pod
kubectl delete pod watcher-test

# Check current kubectl context
kubectl config current-context

