#!/bin/bash
set -euo pipefail

NAMESPACE="sleeve-system"
STATEFULSET_NAME="minio"
PORT_LOCAL=9000
PORT_REMOTE=9000

# 1. Apply manifests
echo "📦 Applying manifests..."
kubectl apply -f .

# 2. Wait for the StatefulSet to be ready
echo "⏳ Waiting for StatefulSet $STATEFULSET_NAME to be ready..."
kubectl rollout status statefulset/$STATEFULSET_NAME -n "$NAMESPACE"

# 3. Get the first pod in the StatefulSet (typically -0)
echo "🔍 Getting pod name..."
POD_NAME=$(kubectl get pod -n "$NAMESPACE" -l statefulset.kubernetes.io/pod-name="$STATEFULSET_NAME-0" -o jsonpath='{.items[0].metadata.name}')

# 4. Start port-forwarding in the background
echo "🚪 Starting port-forward to $POD_NAME..."
kubectl port-forward -n "$NAMESPACE" pod/"$POD_NAME" "$PORT_LOCAL":"$PORT_REMOTE" &
PORT_FORWARD_PID=$!

cleanup () {
    echo "🧹 Killing port-forward process ($PORT_FORWARD_PID)"
    kill "$PORT_FORWARD_PID"
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 2

# 6. Run your command against the forwarded port
echo "🧪 Sending request to localhost:$PORT_LOCAL..."
mc alias set local http://localhost:9000 sleeveadmin sleevepassword
mc admin user add local sleeve sleevesecretkey
mc admin policy attach local readwrite --user sleeve
mc admin user svcacct add local sleeve --access-key "myaccesskey" --secret-key "mysecretkey"

echo "✅ Done"