# AWS S3 Files PoC

Proof-of-concept for [AWS S3 Files](https://aws.amazon.com/s3/features/s3-files/) (launched April 2026), which mounts S3 buckets as NFS 4.2 file systems. This project deploys the full infrastructure with CDK and includes a web-based file browser.

## What it does

- Provisions S3 Files file systems (`AWS::S3Files::FileSystem`) and mount targets via CloudFormation
- Mounts multiple S3 buckets as NFS directories on an EC2 instance
- Serves a **web file browser** (Python, stdlib + boto3) on port 80 with:
  - Multi-bucket landing page (reads `/proc/mounts` — zero NFS calls, instant)
  - Async directory listing (page loads instantly with spinner, JS fetches via API)
  - **S3 API-powered search** with prefix scoping (boto3 paginator, not NFS — handles 1M+ file buckets)
  - Upload, download, create folder, delete operations
  - Dark theme UI

## Architecture

```
┌─────────────┐     NFS 4.2      ┌──────────────────┐      S3 API      ┌─────────┐
│  EC2 (t3.m) │ ◄──────────────► │  S3 Files Mount  │ ◄──────────────► │   S3    │
│  File       │                  │  Targets         │                  │ Buckets │
│  Browser    │                  └──────────────────┘                  └─────────┘
│  :80        │──── boto3 (search) ──────────────────────────────────────►
└─────────────┘
```

- **CDK Stack** creates: VPC security groups, IAM roles (file system + EC2), S3 Files file systems + mount targets, EC2 instance with UserData
- **File Browser** (`filebrowser.py`) is deployed to EC2 via SSM and runs as a systemd service
- **Search** bypasses NFS entirely — queries S3's `ListObjectsV2` API via boto3 paginator with prefix scoping for speed

## Key Design Decisions

| Challenge | Solution |
|---|---|
| S3 Files NFS metadata import is slow on first access to large directories | Async page loading: HTML returns instantly with spinner, JS fetches `/api/ls` endpoint |
| Landing page hung when calling `os.listdir()` on mounts | Read `/proc/mounts` instead — zero NFS syscalls |
| Search via `os.walk()` over NFS extremely slow on 1M+ files | Bypass NFS, query S3 API directly via boto3 paginator |
| Searching entire large bucket still slow (~70s for 1M objects) | Prefix scoping: search from a subfolder only scans that S3 prefix |
| Single-threaded server blocked on slow NFS calls | `ThreadingMixIn` for concurrent request handling |

## Prerequisites

- AWS account with S3 Files enabled (us-east-1)
- Node.js 18+ and npm
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS CLI configured with appropriate credentials
- An existing VPC (or modify the stack to create one)

## Setup

```bash
# Install dependencies
npm install

# Update lib/poc-s3-files-stack.ts with your:
#   - VPC ID
#   - Subnet ID
#   - Bucket names to connect

# Enable versioning on existing buckets (required by S3 Files)
aws s3api put-bucket-versioning --bucket <your-bucket> \
  --versioning-configuration Status=Enabled

# Deploy
npx cdk deploy
```

## Post-Deploy: File Browser

The file browser is deployed via SSM after the stack is up:

```bash
# Get instance ID from stack outputs
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name PocS3FilesStack \
  --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" \
  --output text)

# Deploy filebrowser.py (base64 encode → SSM → write → systemd)
# See the deploy pattern in the project for details

# Access via the instance's public IP on port 80
```

## Project Structure

```
├── lib/poc-s3-files-stack.ts   # CDK stack — S3 Files infra, IAM, EC2
├── bin/poc-s3-files.ts         # CDK app entry point
├── filebrowser.py              # Web file browser (Python stdlib + boto3)
├── cdk.json                    # CDK configuration
└── package.json                # Node.js dependencies
```

## File Browser API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Multi-bucket landing page |
| `GET /<bucket>/[path/]` | Directory listing (async) |
| `GET /<bucket>/[path/]?q=term` | Search (async, S3 API) |
| `GET /api/ls?path=...` | JSON directory listing |
| `GET /api/search?bucket=...&q=...&prefix=...` | JSON search results |
| `GET /<bucket>/path/file` | File download |
| `POST /<bucket>/path/` | Upload, mkdir, delete |

## CDK Commands

- `npx cdk deploy` — deploy the stack
- `npx cdk diff` — compare deployed stack with local changes
- `npx cdk synth` — emit the CloudFormation template
- `npx cdk destroy` — tear down the stack

## Notes

- S3 Files requires **bucket versioning** enabled on all connected buckets
- Changes made via NFS sync to S3 within ~1 minute
- First access to large S3 directories is slow (NFS metadata import) — the async UI pattern handles this gracefully
- The file browser uses the EC2 instance's IAM role for both NFS mounts (`AmazonS3FilesClientFullAccess`) and S3 API search (boto3)
