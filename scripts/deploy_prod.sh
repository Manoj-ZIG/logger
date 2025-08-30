#!/bin/bash

# Exit immediately on error
set -e

# Set AWS CLI region
export AWS_DEFAULT_REGION="us-east-1"

# Define SSM Parameter names
PASSWORD_PARAM="Revmaxai_EC2_Prod_Password"
ENVIRONMENT_PARAM="Revmaxai_EC2_Prod_Password_Environment"

echo "Fetching deployment parameters from AWS SSM..."

# Fetch parameters securely from AWS SSM Parameter Store
PASSWORD=$(aws ssm get-parameter --name "$PASSWORD_PARAM" --with-decryption --query "Parameter.Value" --output text)
ENVIRONMENT=$(aws ssm get-parameter --name "$ENVIRONMENT_PARAM" --with-decryption --query "Parameter.Value" --output text)

# Verify fetched values (optional debug, remove in production)
echo "Environment: $ENVIRONMENT"

# Ensure values are not empty
if [ -z "$PASSWORD" ] || [ -z "$ENVIRONMENT" ]; then
  echo "ERROR: Failed to fetch PASSWORD or ENVIRONMENT from SSM."
  exit 1
fi

# Set base deployment path
BASE_DIR="/home/revmaxai_data"

# Navigate to base directory
cd "$BASE_DIR"

# Copy the appropriate .env file
echo "$PASSWORD" | sudo -S cp ".env_${ENVIRONMENT}" .env

# --------------------------
# vital_lab_extraction setup
# --------------------------
echo "Installing dependencies for vital_lab_extraction..."
cd "$BASE_DIR/vital_lab_extraction/vital_lab_extraction"

# Upgrade pip and install requirements
echo "$PASSWORD" | sudo -S python3 -m pip install --upgrade pip
echo "$PASSWORD" | sudo -S pip3 install --user -r requirements_ec2.txt

# --------------------------
# raw_data_postprocess setup
# --------------------------
echo "Installing dependencies for raw_data_postprocess..."
cd "$BASE_DIR/raw_data_postprocess/raw_data_postprocess"

# Install requirements
echo "$PASSWORD" | sudo -S pip3 install --user -r requirements_ec2.txt

# Final message
echo "✅ Deployment complete for '$ENVIRONMENT' environment."
