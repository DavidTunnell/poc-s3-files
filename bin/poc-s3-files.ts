#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { PocS3FilesStack } from '../lib/poc-s3-files-stack';

const app = new cdk.App();
new PocS3FilesStack(app, 'PocS3FilesStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1',
  },
});
