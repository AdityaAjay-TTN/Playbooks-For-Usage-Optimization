import csv
import sys
import json
import os
import datetime
import boto3


def dbinstance(neptune_client):    
    db_instance = neptune_client.describe_db_instances(
        Filters=[
            {
                'Name': 'engine',
                'Values': [
                    'neptune',
                ]
            },
        ],
    )
    return db_instance["DBInstances"]


def dbcluster(neptune_client):
    db_cluster = neptune_client.describe_db_clusters(
        Filters=[
            {
                'Name': 'engine',
                'Values': [
                    'neptune',
                ]
            },
        ],
    )
    print(db_cluster)


def CPU(cloudwatch_client, dbidentifier,start, end, period):
    response = cloudwatch_client.get_metric_statistics(
                Namespace = 'AWS/Neptune', MetricName = 'CPUUtilization',
                Dimensions = [
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': dbidentifier
                        }
                    ],
                StartTime = start, EndTime = end, Period = period,
                Statistics = [
                        'Average', 'Maximum'
                    ],
                Unit = 'Percent'
            )
    if response['Datapoints']:
        return(response['Datapoints'][0]['Average'], response['Datapoints'][0]['Maximum'])
    else:
        return (0, 0)
    

def Connection_count(cloudwatch_client, dbidentifier, start, end, period):
    response = cloudwatch_client.get_metric_statistics(
                Namespace = 'AWS/Neptune', MetricName = 'TotalRequestsPerSec',
                Dimensions = [
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': dbidentifier
                        }
                    ],
                StartTime = start, EndTime = end, Period = period,
                Statistics = ['Sum']
            )
    if response['Datapoints']:
        return(response['Datapoints'][0]['Sum'])
    else:
        return 0



def get_region_name(region_code):
    try:
        region_mapping = {'us-east-1': 'US East (N. Virginia)', 'us-east-2': 'US East (Ohio)',
                          'us-west-1': 'US West (N. California)', 'us-west-2': 'US West (Oregon)',
                          'af-south-1': 'Africa (Cape Town)',
                          'ap-northeast-1': 'Asia Pacific (Tokyo)', 'ap-northeast-2': 'Asia Pacific (Seoul)',
                          'ap-northeast-3': 'Asia Pacific (Osaka-Local)',
                          'ap-southeast-1': 'Asia Pacific (Singapore)', 'ap-southeast-2': 'Asia Pacific (Sydney)',
                          'ap-east-1': 'Asia Pacific (Hong Kong)',
                          'ap-south-1': 'Asia Pacific (Mumbai)',
                          'ca-central-1': 'Canada (Central)',
                          'eu-central-1': 'EU (Frankfurt)',
                          'eu-west-1': 'EU (Ireland)', 'eu-west-2': 'EU (London)', 'eu-west-3': 'EU (Paris)',
                          'eu-north-1': 'EU (Stockholm)',
                          'eu-south-1': 'EU (Milan)',
                          'me-south-1': 'Middle East (Bahrain)',
                          'sa-east-1': 'South America (Sao Paulo)',
                          'ap-south-2' : 'Asia Pacific (Hyderabad)'
                          }
        return region_mapping.get(region_code)
    except Exception as e:
        return 'US East (N. Virginia)'


def get_price(data):
    od = json.loads(data['PriceList'][0])['terms']['OnDemand']
    id1 = list(od)[0]
    id2 = list(od[id1]['priceDimensions'])[0]
    price = float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])
    return price


def Neptuneprice(pricing_client,instancetype,region):
    neptune_filter = '[{{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},' \
                         '{{"Field": "regionCode", "Value": "{r}", "Type": "TERM_MATCH"}}]'
    try:
        f = neptune_filter.format(t=instancetype,r=get_region_name(region))
        data = pricing_client.get_products(ServiceCode='AmazonNeptune' ,Filters=json.loads(f))
        price = get_price(data)
        return float(price)
    except Exception as e:
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        data = "Message  " + str(e)
        subject = "Getting issue in pricing neptune_finding.py"
        print(data, subject)



def main():

    region = sys.argv[1]
    sts_client = boto3.client('sts')
    neptune_client = boto3.client('neptune',region_name = region) 
    cloudwatch_client = boto3.client('cloudwatch', region_name = region)
    pricing_client = boto3.client('pricing', region_name = region)
    response = sts_client.get_caller_identity()
    accountId = response['Account']
    filename = 'neptune.csv'
    cw_metrics_days = 14
    now = datetime.datetime.utcnow()
    start = (now - datetime.timedelta(days=cw_metrics_days)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    cloudwatch_metrics_period = 1209600
    day = str(cw_metrics_days) + " Days"


    headers = ["Account Id", "Region","DBCluster","DBIdentifier","Engine","Status","VPCId","InstanceClass","MaxCPU("+day+")","AvgCPU("+day+")", "SumOfConnections","Price($)"]
    with open(filename, 'w') as csvFile:
        writer = csv.DictWriter(csvFile, delimiter=',', lineterminator='\n', fieldnames=headers)
        writer.writeheader()

    with open(filename, 'a') as csvFile:
        try:
            writer = csv.DictWriter(csvFile, delimiter=',', lineterminator='\n', fieldnames=headers)
            for instance in dbinstance(neptune_client):
                if Connection_count(instance['DBInstanceIdentifier'],start, end, cloudwatch_metrics_period) >= 1:
                    dbcluster = instance['DBClusterIdentifier']
                    dbidentifier = instance['DBInstanceIdentifier']
                    engine = instance['Engine']
                    dbclass = instance['DBInstanceClass']
                    vpcid = instance['DBSubnetGroup']['VpcId']
                    status = instance['VpcSecurityGroups'][0]['Status']

                    AvgCPU, MaxCPU = CPU(cloudwatch_client,instance["DBInstanceIdentifier"],start, end, cloudwatch_metrics_period)
                    connCount = Connection_count(cloudwatch_client,instance["DBInstanceIdentifier"],start, end, cloudwatch_metrics_period)
                    price = Neptuneprice(pricing_client,instance['DBInstanceClass'],region)

                    
                    row_instance = {} 
                    row_instance.update({"Account Id": accountId}) 
                    row_instance.update({"Region": region})
                    row_instance.update({"DBCluster": dbcluster})
                    row_instance.update({"DBIdentifier": dbidentifier})
                    row_instance.update({"Engine": engine})
                    row_instance.update({"Status": status})
                    row_instance.update({"VPCId": vpcid})
                    row_instance.update({"AvgCPU("+day+")": float("{:.2f}".format(AvgCPU))})
                    row_instance.update({"MaxCPU("+day+")": float("{:.2f}".format(MaxCPU))})
                    row_instance.update({"SumOfConnections": connCount})
                    row_instance.update({"InstanceClass": dbclass})
                    row_instance.update({"Price($)": price})
                    writer.writerow(row_instance)
                    csvFile.flush()
                else:
                    continue
        except Exception as e:
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
            data = "Message  " + str(e)
            subject = "Getting issue in inventory neptune.py"
            print(data, subject)


if __name__ == "__main__":
    main()