import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export class PocS3FilesStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // --- 1. Use existing CloudSeeDrive-Test VPC (account at VPC limit) ---
    const vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', {
      vpcId: 'vpc-0c27f04a8a26fcad3',
    });

    // --- 2. S3 Bucket (versioning required for S3 Files) ---
    const bucket = new s3.Bucket(this, 'S3FilesBucket', {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // --- 3. Security Groups ---
    const ec2Sg = new ec2.SecurityGroup(this, 'Ec2Sg', {
      vpc,
      description: 'EC2 instance security group',
      allowAllOutbound: true,
    });

    const mountTargetSg = new ec2.SecurityGroup(this, 'MountTargetSg', {
      vpc,
      description: 'S3 Files mount target security group',
    });

    // NFS traffic (TCP 2049) from EC2 to mount target
    mountTargetSg.addIngressRule(ec2Sg, ec2.Port.tcp(2049), 'NFS from EC2');

    // SSH access (restrict to your IP in production)
    ec2Sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22), 'SSH access');

    // HTTP access for web file browser
    ec2Sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'HTTP access');

    // --- 4. IAM Role for S3 Files File System ---
    const fileSystemRole = new iam.Role(this, 'S3FilesFileSystemRole', {
      assumedBy: new iam.ServicePrincipal('elasticfilesystem.amazonaws.com'),
    });

    // Add trust policy conditions via escape hatch
    const cfnFsRole = fileSystemRole.node.defaultChild as iam.CfnRole;
    cfnFsRole.addPropertyOverride('AssumeRolePolicyDocument.Statement.0.Condition', {
      StringEquals: { 'aws:SourceAccount': this.account },
      ArnLike: { 'aws:SourceArn': `arn:aws:s3files:${this.region}:${this.account}:file-system/*` },
    });

    // --- Existing buckets to connect ---
    const existingBucketNames = [
      'henry-drive-test-1000k',
      'cloudsee-demo',
      'cloudsee-demo-1',
      'cloudsee-demo-2',
    ];
    const existingBuckets = existingBucketNames.map((name, i) =>
      s3.Bucket.fromBucketName(this, `ExistingBucket${i}`, name)
    );

    // All bucket ARNs (CDK-created + existing)
    const allBucketArns = [bucket.bucketArn, ...existingBuckets.map(b => b.bucketArn)];
    const allObjectArns = [bucket.arnForObjects('*'), ...existingBuckets.map(b => b.arnForObjects('*'))];

    // S3 bucket-level permissions (all buckets)
    fileSystemRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket', 's3:ListBucketVersions'],
      resources: allBucketArns,
    }));

    // S3 object-level permissions (all buckets)
    fileSystemRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:AbortMultipartUpload',
        's3:DeleteObject',
        's3:DeleteObjectVersion',
        's3:GetObject',
        's3:GetObjectVersion',
        's3:GetObjectAttributes',
        's3:ListMultipartUploadParts',
        's3:PutObject',
      ],
      resources: allObjectArns,
    }));

    // EventBridge permissions for S3 Files sync rules
    fileSystemRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'events:DeleteRule',
        'events:DisableRule',
        'events:EnableRule',
        'events:PutRule',
        'events:PutTargets',
        'events:RemoveTargets',
      ],
      resources: [`arn:aws:events:*:${this.account}:rule/DO-NOT-DELETE-S3-Files*`],
      conditions: {
        StringEquals: { 'events:ManagedBy': 'elasticfilesystem.amazonaws.com' },
      },
    }));

    fileSystemRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'events:DescribeRule',
        'events:ListRuleNamesByTarget',
        'events:ListRules',
        'events:ListTargetsByRule',
      ],
      resources: [`arn:aws:events:*:${this.account}:rule/*`],
    }));

    // --- 5. IAM Role for EC2 Instance ---
    const ec2Role = new iam.Role(this, 'Ec2InstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FilesClientFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });

    // Direct S3 read for large file streaming (all buckets)
    bucket.grantRead(ec2Role);
    existingBuckets.forEach(b => b.grantRead(ec2Role));

    // --- 6. S3 Files File Systems ---
    const subnetId = 'subnet-06d30247b5d487c64'; // CloudSeeDrive-Test-subnet-public1-us-east-1a

    // 6a. CDK-created bucket file system
    const fileSystem = new cdk.CfnResource(this, 'S3FilesFileSystem', {
      type: 'AWS::S3Files::FileSystem',
      properties: {
        Bucket: bucket.bucketArn,
        RoleArn: fileSystemRole.roleArn,
        AcceptBucketWarning: true,
      },
    });

    const mountTarget = new cdk.CfnResource(this, 'S3FilesMountTarget', {
      type: 'AWS::S3Files::MountTarget',
      properties: {
        FileSystemId: fileSystem.getAtt('FileSystemId').toString(),
        SubnetId: subnetId,
        SecurityGroups: [mountTargetSg.securityGroupId],
      },
    });
    mountTarget.addDependency(fileSystem);

    // 6b. File systems for existing buckets
    const existingFileSystems: { name: string; fsId: string }[] = [];
    existingBucketNames.forEach((bucketName, i) => {
      const fs = new cdk.CfnResource(this, `ExtFS${i}`, {
        type: 'AWS::S3Files::FileSystem',
        properties: {
          Bucket: `arn:aws:s3:::${bucketName}`,
          RoleArn: fileSystemRole.roleArn,
          AcceptBucketWarning: true,
        },
      });

      const mt = new cdk.CfnResource(this, `ExtMT${i}`, {
        type: 'AWS::S3Files::MountTarget',
        properties: {
          FileSystemId: fs.getAtt('FileSystemId').toString(),
          SubnetId: subnetId,
          SecurityGroups: [mountTargetSg.securityGroupId],
        },
      });
      mt.addDependency(fs);

      existingFileSystems.push({ name: bucketName, fsId: fs.getAtt('FileSystemId').toString() });

      // Output each file system ID
      new cdk.CfnOutput(this, `ExtFSId${i}`, {
        value: fs.getAtt('FileSystemId').toString(),
        description: `File system ID for ${bucketName}`,
      });
    });

    // --- 8. EC2 Instance (same subnet as mount target) ---
    const subnet = ec2.Subnet.fromSubnetAttributes(this, 'PublicSubnet', {
      subnetId,
      availabilityZone: 'us-east-1a',
    });
    const instance = new ec2.Instance(this, 'S3FilesInstance', {
      vpc,
      vpcSubnets: { subnets: [subnet] },
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      machineImage: ec2.MachineImage.latestAmazonLinux2023(),
      securityGroup: ec2Sg,
      role: ec2Role,
    });
    // Set public IP via escape hatch (CDK validation blocks it for imported subnets)
    const cfnInstance = instance.instance;
    cfnInstance.addPropertyOverride('NetworkInterfaces', [{
      AssociatePublicIpAddress: true,
      DeviceIndex: '0',
      SubnetId: subnetId,
      GroupSet: [ec2Sg.securityGroupId],
    }]);
    // Remove top-level properties that conflict with NetworkInterfaces
    cfnInstance.addPropertyDeletionOverride('SubnetId');
    cfnInstance.addPropertyDeletionOverride('SecurityGroupIds');

    // Ensure EC2 waits for all mount targets
    instance.node.addDependency(mountTarget);

    // UserData: install NFS utils, mount all S3 Files file systems
    const pocFsId = fileSystem.getAtt('FileSystemId').toString();
    instance.addUserData(
      '#!/bin/bash',
      'set -euxo pipefail',
      'exec > >(tee /var/log/s3files-setup.log) 2>&1',
      '',
      '# Install amazon-efs-utils (includes mount.s3files helper)',
      'dnf install -y amazon-efs-utils',
      '',
      '# Mount helper function',
      'mount_s3files() {',
      '    local FSID=$1 MNTPT=$2',
      '    mkdir -p "$MNTPT"',
      '    for i in $(seq 1 30); do',
      '        if mount -t s3files "${FSID}:/" "$MNTPT" 2>/dev/null; then',
      '            echo "Mounted $FSID at $MNTPT (attempt $i)"',
      '            echo "${FSID}:/ $MNTPT s3files _netdev,nofail 0 0" >> /etc/fstab',
      '            return 0',
      '        fi',
      '        echo "  Attempt $i/30 failed for $MNTPT, retrying..."',
      '        sleep 10',
      '    done',
      '    echo "ERROR: Failed to mount $FSID at $MNTPT"',
      '    return 1',
      '}',
      '',
      '# Mount all file systems under /mnt/s3files/<bucket-name>',
      `mount_s3files "${pocFsId}" "/mnt/s3files/poc-bucket"`,
      ...existingFileSystems.map(({ name, fsId }) =>
        `mount_s3files "${fsId}" "/mnt/s3files/${name}"`
      ),
      '',
      '# Test write on PoC bucket',
      'echo "Hello from S3 Files PoC! $(date)" > /mnt/s3files/poc-bucket/poc-test.txt',
      '',
      'echo "All S3 Files mounts complete."',
      'df -h | grep s3files',
    );

    // --- 9. Stack Outputs ---
    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'FileSystemId', { value: fileSystem.getAtt('FileSystemId').toString() });
    new cdk.CfnOutput(this, 'InstanceId', { value: instance.instanceId });
    new cdk.CfnOutput(this, 'InstancePublicIp', { value: instance.instancePublicIp });
    new cdk.CfnOutput(this, 'SSMConnectCommand', {
      value: `aws ssm start-session --target ${instance.instanceId} --region ${this.region}`,
    });
  }
}
