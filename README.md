# Project Setup Instructions

## Buckets Creation
Create the following S3 buckets:

- **s3-raw-data-ec2-021095**
- **s3-summarized-data-ec2-021095**
- **s3-consolidated-data-ec2-021095**
- **s3-raw-data-lambda-021095**
- **s3-summarized-data-lambda-021095**
- **s3-consolidated-data-ec2-021095**

---

## Workers Deployment
1. **Compile the workers**:
   - Ensure that all worker code is compiled.

2. **Deploy EC2 workers**:
   - Place each EC2 worker on a separate instance.
   - Start the workers manually or using the appropriate deployment script.

---

## Lambda Functions Setup
1. **Upload Lambda functions to AWS**:
   - Increase the timeouts and memory allocation in the function settings.
   - Configure the input function correctly.

2. **Event Triggers**:
   - Configure the Lambda functions to be triggered by `PUT` events in the associated S3 buckets.

---

## SQS Queue Configuration
- Create an **SQS queue** linked to the bucket `s3-summarized-data-ec2-021095` as the input source.

---

## EC2 Configuration
1. **AWS Credentials**:
   - Ensure proper AWS credentials are configured on all EC2 instances.

2. **Dependencies**:
   - Install the necessary dependencies for running Java on the EC2 instances.

---

## Running the Project
1. **Start the Upload Function**:
   - Run the upload function to transfer raw data files to the `s3-raw-data` bucket.

2. **Run the Export Script**:
   - Once the upload is complete, run the export script.
   - The results will be stored in the root directory of the project.
