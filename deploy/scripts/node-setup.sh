#!/usr/bin/env bash
# node-setup.sh — bootstrap k3s and ECR credential helper on tokyo-1.
#
# Run once, as the default ec2-user (or ubuntu), from your local machine:
#
#   ssh -i "~/Documents/Documents - MacBook Air de Simon/tokyo-1-secrets.pem" \
#       ec2-user@35.77.39.5 'bash -s' < deploy/scripts/node-setup.sh
#
# Or copy the script first and run it on the node directly.
set -euo pipefail

ECR_REGION="ap-northeast-1"
ECR_REGISTRY="931944281606.dkr.ecr.ap-northeast-1.amazonaws.com"

echo "==> [1/5] System update"
sudo yum update -y 2>/dev/null || sudo apt-get update -y

echo "==> [2/5] Install AWS CLI v2"
if ! command -v aws &>/dev/null; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o /tmp/awscliv2.zip
  cd /tmp && unzip -q awscliv2.zip
  sudo ./aws/install
  rm -rf /tmp/aws /tmp/awscliv2.zip
  cd -
fi
echo "  AWS CLI: $(aws --version)"

echo "==> [3/5] Install k3s (single-node, no traefik)"
# --disable traefik: we don't need an ingress controller for this workload.
# INSTALL_K3S_EXEC env passes flags that persist across upgrades.
export INSTALL_K3S_EXEC="--disable traefik --write-kubeconfig-mode 644"
curl -sfL https://get.k3s.io | sh -

# Wait for k3s to be ready.
sleep 5
sudo k3s kubectl wait --for=condition=Ready node --all --timeout=120s

# Allow the current user to run kubectl without sudo.
mkdir -p "$HOME/.kube"
sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
echo "  k3s version: $(k3s --version | head -1)"

echo "==> [4/5] Configure k3s to pull from ECR"
# k3s reads /etc/rancher/k3s/registries.yaml for private registry credentials.
# The credential helper refreshes the token every 12 h automatically.
sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/registries.yaml > /dev/null <<YAML
mirrors:
  "${ECR_REGISTRY}":
    endpoint:
      - "https://${ECR_REGISTRY}"
configs:
  "${ECR_REGISTRY}":
    auth:
      username: AWS
      password: "$(aws ecr get-login-password --region ${ECR_REGION})"
YAML

# Restart k3s so it picks up the registry config.
sudo systemctl restart k3s
sleep 5

# Install a cron job to refresh the ECR token every 6 h (tokens expire in 12 h).
(crontab -l 2>/dev/null; echo "0 */6 * * * \
  sudo sed -i \"s|password:.*|password: \\\"\$(aws ecr get-login-password --region ${ECR_REGION})\\\"|\" \
  /etc/rancher/k3s/registries.yaml && sudo systemctl restart k3s") | crontab -

echo "==> [5/5] Label the node so the connector pod can pin to it"
kubectl label node "$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')" \
  kubernetes.io/hostname=tokyo-1 --overwrite

echo ""
echo "✓ Node setup complete."
echo ""
echo "Next steps:"
echo "  1. Run deploy/scripts/aws-setup.sh from your local machine"
echo "  2. Add GitHub secrets (see aws-setup.sh output)"
echo "  3. kubectl apply -f deploy/k8s/"
echo "  4. Wait for ClickHouse to be Ready, then:"
echo "     kubectl apply -f deploy/k8s/schema-job.yaml"
echo "  5. Push to main — GitHub Actions will build and deploy the connector"
