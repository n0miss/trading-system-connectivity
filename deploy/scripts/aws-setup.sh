#!/usr/bin/env bash
# aws-setup.sh — create ECR repositories and GitHub Actions OIDC IAM role.
#
# Run ONCE from your local machine with AWS credentials for account 931944281606:
#   AWS_PROFILE=your-profile bash deploy/scripts/aws-setup.sh
#
# After this runs, add two secrets to the GitHub repo
# (Settings → Secrets → Actions) as shown at the end of the script output.
set -euo pipefail

AWS_ACCOUNT="931944281606"
AWS_REGION="ap-northeast-1"
GITHUB_ORG="n0miss"
GITHUB_REPO="trading-system-connectivity"
ROLE_NAME="github-actions-trading-deploy"
ECR_REPOS=("trading/connector" "trading/aeron-driver" "trading/clickhouse-bridge")

echo "==> [1/4] Create ECR repositories (idempotent)"
for repo in "${ECR_REPOS[@]}"; do
  aws ecr describe-repositories \
    --repository-names "$repo" \
    --region "$AWS_REGION" &>/dev/null \
  && echo "  [exists] $repo" \
  || {
    aws ecr create-repository \
      --repository-name "$repo" \
      --region "$AWS_REGION" \
      --image-scanning-configuration scanOnPush=true \
      --query 'repository.repositoryUri' \
      --output text
    echo "  [created] $repo"
  }
done

echo ""
echo "==> [2/4] Create/update OIDC provider for GitHub Actions"
OIDC_URL="https://token.actions.githubusercontent.com"
OIDC_ARN="arn:aws:iam::${AWS_ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"

aws iam get-open-id-connect-provider --open-id-connect-provider-arn "$OIDC_ARN" &>/dev/null \
&& echo "  [exists] OIDC provider" \
|| {
  THUMBPRINT=$(openssl s_client -connect token.actions.githubusercontent.com:443 \
    -servername token.actions.githubusercontent.com </dev/null 2>/dev/null \
    | openssl x509 -fingerprint -sha1 -noout \
    | sed 's/://g;s/.*=//' \
    | tr '[:upper:]' '[:lower:]')

  aws iam create-open-id-connect-provider \
    --url "$OIDC_URL" \
    --thumbprint-list "$THUMBPRINT" \
    --client-id-list "sts.amazonaws.com"
  echo "  [created] OIDC provider (thumbprint: $THUMBPRINT)"
}

echo ""
echo "==> [3/4] Create/update IAM role '${ROLE_NAME}'"
TRUST_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${AWS_ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:${GITHUB_ORG}/${GITHUB_REPO}:*"
        },
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
JSON
)

ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT}:role/${ROLE_NAME}"

aws iam get-role --role-name "$ROLE_NAME" &>/dev/null \
&& {
  aws iam update-assume-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-document "$TRUST_POLICY"
  echo "  [updated] trust policy on existing role"
} \
|| {
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    --description "GitHub Actions deploy role for ${GITHUB_ORG}/${GITHUB_REPO}" \
    --query 'Role.Arn' --output text
  echo "  [created] role ${ROLE_NAME}"
}

echo ""
echo "==> [4/4] Attach ECR permissions to the role"
ECR_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECRAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "ECRReadWrite",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchGetImage",
        "ecr:BatchCheckLayerAvailability",
        "ecr:CompleteLayerUpload",
        "ecr:GetDownloadUrlForLayer",
        "ecr:InitiateLayerUpload",
        "ecr:PutImage",
        "ecr:UploadLayerPart",
        "ecr:DescribeRepositories",
        "ecr:ListImages"
      ],
      "Resource": [
        "arn:aws:ecr:${AWS_REGION}:${AWS_ACCOUNT}:repository/trading/*"
      ]
    }
  ]
}
JSON
)

POLICY_NAME="${ROLE_NAME}-ecr"
POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT}:policy/${POLICY_NAME}"

aws iam get-policy --policy-arn "$POLICY_ARN" &>/dev/null \
&& {
  # Update the default version of an existing policy.
  VERSION_ID=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
    --query 'Versions[?!IsDefaultVersion].VersionId' --output text | head -1)
  [[ -n "$VERSION_ID" ]] && aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$VERSION_ID"
  aws iam create-policy-version \
    --policy-arn "$POLICY_ARN" \
    --policy-document "$ECR_POLICY" \
    --set-as-default
  echo "  [updated] policy ${POLICY_NAME}"
} \
|| {
  POLICY_ARN=$(aws iam create-policy \
    --policy-name "$POLICY_NAME" \
    --policy-document "$ECR_POLICY" \
    --query 'Policy.Arn' --output text)
  echo "  [created] policy ${POLICY_NAME}"
}

aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "$POLICY_ARN" 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✓ AWS setup complete."
echo ""
echo "Add these three secrets to GitHub:"
echo "  Repo: https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/settings/secrets/actions"
echo ""
echo "  Name:  AWS_ROLE_ARN"
echo "  Value: ${ROLE_ARN}"
echo ""
echo "  Name:  DEPLOY_HOST"
echo "  Value: 35.77.39.5"
echo ""
echo "  Name:  TOKYO1_SSH_KEY"
echo "  Value: (contents of ~/Documents/Documents - MacBook Air de Simon/tokyo-1-secrets.pem)"
echo "         cat \"/path/to/tokyo-1-secrets.pem\" | pbcopy   # then paste"
echo ""
echo "AWS credentials use OIDC (no static keys stored in GitHub)."
