$AMI="ami-"
$KEY="key"
$SG="sg-"
$SUBNET="subnet-"

aws ec2 run-instances `
  --image-id $AMI `
  --instance-type t3.micro `
  --key-name $KEY `
  --security-group-ids $SG `
  --subnet-id $SUBNET `
  --user-data file://ondemand-worker-user-data.sh `
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dask-ondemand-worker}]' `
  --count 1