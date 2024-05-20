import boto3
import csv,os,json,sys
from datetime import datetime, timedelta, timezone


def describe_ebs(ebs_client):
    response = ebs_client.describe_volumes()
    return response['Volumes']


def get_sum_metric(cw_client, namespace, metric_name, dimension_name, dimension_value, cw_metrics_days,cloudwatch_metrics_period):
    now = datetime.utcnow()
    start_time = (now - timedelta(days=cw_metrics_days)).strftime('%Y-%m-%d')
    end_time = now.strftime('%Y-%m-%d')
    response = cw_client.get_metric_statistics(
        Namespace=namespace, MetricName=metric_name,
        Dimensions=[
            {
                'Name': dimension_name,
                'Value': dimension_value
            },
        ],
        StartTime = start_time, EndTime = end_time, Period = cloudwatch_metrics_period,
        Statistics=['Sum']
    )
    if response['Datapoints']:
        Sum = response['Datapoints'][0]['Sum']
    else:
        Sum = 0
    return Sum



def get_instance_id(ebs_client, volumeId):
    response = ebs_client.describe_volumes(VolumeIds=[volumeId])
    for vol in response['Volumes']:
        instanceId = (vol['Attachments'][0]['InstanceId'])
    return instanceId


def get_ebs_storage_price(pricing_client, volume_type, region):
    ebs_storage_filter = '[{{"Field": "volumeType", "Value": "{vn}", "Type": "TERM_MATCH"}},' \
                     '{{"Field": "productFamily", "Value": "Storage", "Type": "TERM_MATCH"}},' \
                     '{{"Field": "volumeApiName", "Value": "{v}", "Type": "TERM_MATCH"}},' \
                     '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'

    storage_price = 0
    try:
        volume_name = get_volume_name(volume_type)
        f = ebs_storage_filter.format(v=volume_type, r=get_region_name(region), vn=volume_name)
        data = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
        storage_price = get_price(data)
        return float(storage_price)
    except Exception as e:
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        print(str(e))
        return float(storage_price)



def get_throughput_price(pricing_client,region):
    ebs_throughput_filter = '[{{"Field": "group", "Value": "EBS Throughput", "Type": "TERM_MATCH"}},' \
                  '{{"Field": "productFamily", "Value": "Provisioned Throughput", "Type": "TERM_MATCH"}},' \
                  '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'

    throughput_price = 0
    try:
        f = ebs_throughput_filter.format(r=get_region_name(region))
        data = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
        throughput_price = get_price(data)
    except Exception as e:
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        print(str(e))
        return float(throughput_price)
    return throughput_price


def get_volume_name(volume_type):
    ebs_storage_mapping = {'gp2': 'General Purpose', 'gp3': 'General Purpose','io1': 'Provisioned IOPS', 'sc1': 'Cold HDD',
                            'io2': 'Provisioned IOPS', 'st1': 'Throughput Optimized HDD', 'standard': 'Magnetic'}
    return ebs_storage_mapping.get(volume_type)



def get_ebs_iops_price(pricing_client, volume_type, region):
    ebs_iops_filter = '[{{"Field": "group", "Value": "EBS IOPS", "Type": "TERM_MATCH"}},' \
                  '{{"Field": "productFamily", "Value": "System Operation", "Type": "TERM_MATCH"}},' \
                  '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'

    iops_price = 0
    try:
        if volume_type == 'io1' or volume_type == 'io2':
            f = ebs_iops_filter.format(r=get_region_name(region))
            data = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
            iops_price = get_price(data)
    except Exception as e:
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        print(str(e))
        return float(iops_price)
    return iops_price

def get_price(data):
    od = json.loads(data['PriceList'][0])['terms']['OnDemand']
    id1 = list(od)[0]
    id2 = list(od[id1]['priceDimensions'])[0]
    price = float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])
    return price


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
    


def main():

    region = sys.argv[1]
    sts_client = boto3.client('sts')
    ebs_client = boto3.client('ec2',region_name = region) #region specific ec2 client
    cloudwatch_client = boto3.client('cloudwatch', region_name = region)
    pricing_client = boto3.client('pricing', region_name = region)
    response = sts_client.get_caller_identity()
    accountId = response['Account']
    filename = 'ebs.csv'
    cw_metrics_days = 14
    cloudwatch_metrics_period = 1209600


    headers = ["Account ID", "Region", "VolumeId", "State", "InstanceId", "Type", "SnapshotId", "SizeGB", 'Iops',"Throughput","IOPSUtilization",'CreationDate','cloudwatch_period',
                        "CurrentMonthlyStorageCost($)", "CurrentMonthlyIopsCost($)","CurrentMonthlyThroughputCost($)","Finding", "Savings($)"]
    with open(filename, 'w') as csvFile:
        writer = csv.DictWriter(csvFile, delimiter=',', lineterminator='\n', fieldnames=headers)
        writer.writeheader()
    

    with open(filename, 'a') as csvFile:
        writer = csv.DictWriter(csvFile, delimiter=',', lineterminator='\n', fieldnames=headers)
        for vol in describe_ebs(ebs_client):
            instanceId = "NA"
            iops = 0
            finding = ''
            throughput_price=0
            storage_price=0
            iops_price=0
            savings=0
            DiskReadOps=0
            DiskWriteOps=0
            iopsutilization=0

            create_time = vol['CreateTime']
            creation_date = datetime.strptime(str(create_time).split(' ')[0], '%Y-%m-%d').date()
            cloudwatch_period = datetime.now(timezone.utc) - timedelta(days=15)

            #Caculation of Disk Utilization
            DiskReadOps = get_sum_metric(cloudwatch_client, 'AWS/EBS', 'VolumeReadOps',
                                                'VolumeId', vol["VolumeId"], cw_metrics_days,cloudwatch_metrics_period)
            DiskWriteOps = get_sum_metric(cloudwatch_client, 'AWS/EBS', 'VolumeWriteOps',
                                                'VolumeId', vol["VolumeId"], cw_metrics_days,cloudwatch_metrics_period)
            iopsutilization = (DiskReadOps + DiskWriteOps)

            #current ebs storage price & iops price
            if 'Iops' in vol:
                iops = int(vol["Iops"])
                if iops > 3000:
                    iops_gp3 = iops - 3000

            #state check - available
            if vol["State"] == 'available':
                finding = 'Available'
                storage_price = float("{:.2f}".format(get_ebs_storage_price(pricing_client, vol["VolumeType"], region) * float(vol["Size"])))
                if vol['VolumeType'] == 'gp3' and 'Throughput' in vol and vol['Throughput'] > 125:
                        throughput_price = float("{:.2f}".format(get_throughput_price()/1000 * float(vol['Throughput'] - 125)))
                iops_price = float("{:.2f}".format(get_ebs_iops_price(pricing_client,vol["VolumeType"],region) * iops))     
                savings = storage_price + iops_price + throughput_price

            #state check - in-use
            elif vol["State"] == 'in-use' and create_time <= cloudwatch_period:

                if iopsutilization == 0:
                    finding = 'Unused'
                    instanceId = get_instance_id(ebs_client,vol["VolumeId"])
                    ##pricing api Unused
                    storage_price = float("{:.2f}".format(get_ebs_storage_price(pricing_client, vol["VolumeType"], region) * float(vol["Size"])))
                    if vol['VolumeType'] == 'gp3' and 'Throughput' in vol and vol['Throughput'] > 125:
                        throughput_price = float("{:.2f}".format(get_throughput_price()/1000 * float(vol['Throughput'] - 125)))
                    iops_price = float("{:.2f}".format(get_ebs_iops_price(pricing_client,vol["VolumeType"],region) * iops))
                    savings = storage_price + iops_price + throughput_price

                
        

            row_instance = {}
            if finding != '':
                row_instance.update({'Account ID': accountId})
                row_instance.update({'Region': region})
                row_instance.update({"VolumeId": vol["VolumeId"]})
                row_instance.update({"State": vol["State"]})
                row_instance.update({"InstanceId": instanceId})
                row_instance.update({"Type": vol["VolumeType"]})
                row_instance.update({"SnapshotId": vol["SnapshotId"]})
                row_instance.update({"SizeGB": vol["Size"]})
                row_instance.update({"Iops": iops})
                row_instance.update({"cloudwatch_period": cloudwatch_period})
                row_instance.update({"Throughput": vol['Throughput'] if 'Throughput' in vol else 0})
                row_instance.update({"IOPSUtilization": iopsutilization})
                row_instance.update({"CreationDate": creation_date})
                row_instance.update(
                    {"CurrentMonthlyStorageCost($)": storage_price})
                row_instance.update(
                    {"CurrentMonthlyIopsCost($)": iops_price})
                row_instance.update(
                    {"CurrentMonthlyThroughputCost($)": throughput_price})
                row_instance.update(
                    {"Savings($)":savings})
                row_instance.update({"Finding": finding})
                writer.writerow(row_instance)
                csvFile.flush()

        print("Output stored in ebs.csv file.")



if __name__ == "__main__":
    main()