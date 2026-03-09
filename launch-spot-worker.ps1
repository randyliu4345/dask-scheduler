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
  --user-data file://spot-worker-user-data.sh `
  --instance-market-options '{"MarketType":"spot"}' `
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dask-spot-worker}]' `
  --count 1