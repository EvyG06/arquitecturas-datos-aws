export AWS_PROFILE=admin
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION=us-east-2
export PROJECT=arquitecturas-datos
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export DL_BUCKET="eve-dl-${ACCOUNT_ID}-${AWS_REGION}"
export DW_BUCKET="eve-dw-${ACCOUNT_ID}-${AWS_REGION}"
export AWS_PAGER=""
export PYTHONPATH="$PWD/data-lake/src"
export AIRFLOW_INSTANCE=$(aws ec2 describe-instances --filters Name=tag:Name,Values=arq-datos-airflow Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId | [0]' --output text 2>/dev/null)
