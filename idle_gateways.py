#!/usr/bin/env python3
"""
CloudWatch Gateway Metrics Collection Script
Requires Python 3.7+
Tested with Python 3.13.1
"""

import boto3
import datetime
from datetime import timezone
import pandas as pd
import sys
from typing import List, Dict, Tuple
import openpyxl
from openpyxl.utils import get_column_letter


def get_gateway_info(gateway: Dict, ec2) -> Tuple[str, str, str]:
    """Get gateway name, VPC ID, and VPC name from tags"""
    tags = gateway.get('Tags', [])
    name_tag = next((tag['Value'] for tag in tags if tag['Key'] == 'Name'), None)
    vpc_id = None
    vpc_name = None

    # Get VPC information
    if 'VpcId' in gateway:  # For NAT Gateway
        vpc_id = gateway['VpcId']
    elif 'Attachments' in gateway and gateway['Attachments']:  # For IGW
        vpc_id = gateway['Attachments'][0].get('VpcId')

    if vpc_id:
        try:
            vpc = ec2.describe_vpcs(VpcIds=[vpc_id])['Vpcs'][0]
            vpc_name = next((tag['Value'] for tag in vpc.get('Tags', []) if tag['Key'] == 'Name'), vpc_id)
        except:
            vpc_name = vpc_id

    if not name_tag:
        if vpc_name:
            name_tag = f"IGW-{vpc_name}" if 'InternetGatewayId' in gateway else f"NAT-{vpc_name}"
        else:
            name_tag = gateway.get('NatGatewayId', gateway.get('InternetGatewayId', 'Unknown'))

    return name_tag, vpc_id, vpc_name


def get_metric_data_chunked(cloudwatch, namespace: str, metric_name: str,
                            dimensions: List[Dict], start_time: datetime.datetime,
                            end_time: datetime.datetime) -> List[Dict]:
    """Get metric data in chunks to avoid exceeding CloudWatch limits"""
    all_datapoints = []
    period = 21600  # 6-hour period
    chunk_size = datetime.timedelta(days=30)
    chunk_start = start_time

    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_size, end_time)

        response = cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=chunk_start,
            EndTime=chunk_end,
            Period=period,
            Statistics=['Sum', 'Average', 'Maximum', 'Minimum']  # Added Minimum
        )

        all_datapoints.extend(response['Datapoints'])
        chunk_start = chunk_end

    return all_datapoints


def get_account_id(sts_client):
    """Get current AWS account ID"""
    try:
        return sts_client.get_caller_identity()["Account"]
    except Exception as e:
        print(f"Warning: Could not get account ID: {e}")
        return "Unknown"


def get_gateway_metrics(region: str, profile_name: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch CloudWatch metrics for NAT Gateway and Internet Gateway"""
    # Initialize boto3 clients
    session = boto3.Session(profile_name=profile_name, region_name=region)
    cloudwatch = session.client('cloudwatch')
    ec2 = session.client('ec2')
    sts = session.client('sts')

    # Get account ID
    account_id = get_account_id(sts)
    print(f"\nAnalyzing gateways for Account: {account_id}, Region: {region}")

    end_time = datetime.datetime.now(timezone.utc)
    start_time = end_time - datetime.timedelta(days=90)

    nat_gateways = ec2.describe_nat_gateways()['NatGateways']
    internet_gateways = ec2.describe_internet_gateways()['InternetGateways']

    # Extended NAT Gateway metrics
    nat_metrics = [
        'BytesInFromDestination',
        'BytesInFromSource',
        'BytesOutToDestination',
        'BytesOutToSource',
        'PacketsInFromDestination',
        'PacketsInFromSource',
        'PacketsOutToDestination',
        'PacketsOutToSource',
        'ConnectionAttemptCount',
        'ConnectionEstablishedCount',
        'ErrorPortAllocation',
        'IdleTimeoutCount',
        'ActiveConnectionCount',  # Added metric
        'ConnectionEstablishedRate'  # Added metric
    ]

    # Extended Internet Gateway metrics
    igw_metrics = [
        'BytesInFromDestination',  # Updated metric names
        'BytesOutToDestination',
        'PacketsInFromDestination',
        'PacketsOutToDestination',
        'BytesDropCountBlackholeIPv4',
        'BytesDropCountNoRouteIPv4',
        'PacketsDropCountBlackholeIPv4',
        'PacketsDropCountNoRouteIPv4'
    ]

    results = []

    # Collect NAT Gateway metrics
    for nat in nat_gateways:
        nat_id = nat['NatGatewayId']
        nat_name, vpc_id, vpc_name = get_gateway_info(nat, ec2)
        print(f"Collecting metrics for NAT Gateway: {nat_name} ({nat_id})")
        print(f"Associated VPC: {vpc_name} ({vpc_id})")

        for metric in nat_metrics:
            datapoints = get_metric_data_chunked(
                cloudwatch,
                'AWS/NATGateway',
                metric,
                [{'Name': 'NatGatewayId', 'Value': nat_id}],
                start_time,
                end_time
            )

            for datapoint in datapoints:
                results.append({
                    'Account_ID': account_id,
                    'Region': region,
                    'Gateway_Type': 'NAT',
                    'Gateway_ID': nat_id,
                    'Gateway_Name': nat_name,
                    'VPC_ID': vpc_id,
                    'VPC_Name': vpc_name,
                    'Metric': metric,
                    'Timestamp': datapoint['Timestamp'],
                    'Sum': datapoint.get('Sum', 0),
                    'Average': datapoint.get('Average', 0),
                    'Maximum': datapoint.get('Maximum', 0),
                    'Minimum': datapoint.get('Minimum', 0)  # Added Minimum
                })

    # Collect Internet Gateway metrics
    for igw in internet_gateways:
        igw_id = igw['InternetGatewayId']
        igw_name, vpc_id, vpc_name = get_gateway_info(igw, ec2)
        print(f"\nCollecting metrics for Internet Gateway: {igw_name} ({igw_id})")
        print(f"Associated VPC: {vpc_name} ({vpc_id})")

        print("Available metrics:")

        for metric in igw_metrics:
            # First check if metric exists for this IGW
            metric_data = cloudwatch.list_metrics(
                Namespace='AWS/IGW',
                MetricName=metric,
                Dimensions=[{'Name': 'InternetGatewayId', 'Value': igw_id}]
            )

            if metric_data['Metrics']:
                print(f"  - Found {metric}")
                datapoints = get_metric_data_chunked(
                    cloudwatch,
                    'AWS/IGW',
                    metric,
                    [{'Name': 'InternetGatewayId', 'Value': igw_id}],
                    start_time,
                    end_time
                )
            else:
                print(f"  - No data for {metric}")
                datapoints = []

            # Add at least one row for IGW even if no metrics
            if not datapoints:
                results.append({
                    'Account_ID': account_id,
                    'Region': region,
                    'Gateway_Type': 'IGW',
                    'Gateway_ID': igw_id,
                    'Gateway_Name': igw_name,
                    'VPC_ID': vpc_id,
                    'VPC_Name': vpc_name,
                    'Metric': metric,
                    'Timestamp': start_time,  # Use start_time as default timestamp
                    'Sum': 0,
                    'Average': 0,
                    'Maximum': 0,
                    'Minimum': 0,
                    'VPC_ID': vpc_id
                })

            for datapoint in datapoints:
                results.append({
                    'Gateway_Type': 'IGW',
                    'Gateway_ID': igw_id,
                    'Gateway_Name': igw_name,
                    'Metric': metric,
                    'Timestamp': datapoint['Timestamp'],
                    'Sum': datapoint.get('Sum', 0),
                    'Average': datapoint.get('Average', 0),
                    'Maximum': datapoint.get('Maximum', 0),
                    'Minimum': datapoint.get('Minimum', 0)  # Added Minimum
                })

    df = pd.DataFrame(results)

    if df.empty:
        print("No metrics data found for the specified period")
        return pd.DataFrame(), pd.DataFrame()

    def analyze_idle_time(df: pd.DataFrame) -> pd.DataFrame:
        idle_analysis = []

        for gateway_type in ['NAT', 'IGW']:
            gateways = df[df['Gateway_Type'] == gateway_type]['Gateway_ID'].unique()

            for gateway in gateways:
                gateway_data = df[(df['Gateway_Type'] == gateway_type) &
                                  (df['Gateway_ID'] == gateway)]

                gateway_name = gateway_data.iloc[0]['Gateway_Name']

                if gateway_type == 'NAT':
                    traffic_metrics = [m for m in nat_metrics if 'Bytes' in m or 'Packets' in m]
                else:
                    traffic_metrics = [m for m in igw_metrics if 'Bytes' in m or 'Packets' in m]

                total_periods = len(gateway_data['Timestamp'].unique())
                idle_periods = len(gateway_data[
                                       (gateway_data['Metric'].isin(traffic_metrics)) &
                                       (gateway_data['Sum'] == 0)
                                       ]['Timestamp'].unique())

                idle_percentage = (idle_periods / total_periods * 100) if total_periods > 0 else 0

                # Calculate various metrics
                # Get VPC info from the first row
                vpc_id = gateway_data['VPC_ID'].iloc[0] if 'VPC_ID' in gateway_data.columns else 'Unknown'
                vpc_name = gateway_data['VPC_Name'].iloc[0] if 'VPC_Name' in gateway_data.columns else 'Unknown'

                metrics_summary = {
                    'Account_ID': account_id,
                    'Region': region,
                    'VPC_ID': vpc_id,
                    'VPC_Name': vpc_name,
                    'Gateway_Type': gateway_type,
                    'Gateway_ID': gateway,
                    'Gateway_Name': gateway_name,
                    'Total_Periods': total_periods,
                    'Idle_Periods': idle_periods,
                    'Idle_Percentage': round(idle_percentage, 2)
                }

                # Calculate traffic metrics based on gateway type
                if gateway_type == 'NAT':
                    # NAT Gateway bytes
                    metrics_summary.update({
                        'Total_Bytes_In': gateway_data[
                            gateway_data['Metric'].isin(['BytesInFromSource', 'BytesInFromDestination'])
                        ]['Sum'].sum(),
                        'Total_Bytes_Out': gateway_data[
                            gateway_data['Metric'].isin(['BytesOutToSource', 'BytesOutToDestination'])
                        ]['Sum'].sum(),
                        # NAT Gateway packets
                        'Total_Packets_In': gateway_data[
                            gateway_data['Metric'].isin(['PacketsInFromSource', 'PacketsInFromDestination'])
                        ]['Sum'].sum(),
                        'Total_Packets_Out': gateway_data[
                            gateway_data['Metric'].isin(['PacketsOutToSource', 'PacketsOutToDestination'])
                        ]['Sum'].sum(),
                        # NAT Gateway connection metrics
                        'Total_Connection_Attempts': gateway_data[
                            gateway_data['Metric'] == 'ConnectionAttemptCount'
                            ]['Sum'].sum(),
                        'Total_Connection_Timeouts': gateway_data[
                            gateway_data['Metric'] == 'IdleTimeoutCount'
                            ]['Sum'].sum(),
                        'Port_Allocation_Errors': gateway_data[
                            gateway_data['Metric'] == 'ErrorPortAllocation'
                            ]['Sum'].sum(),
                        'Max_Active_Connections': gateway_data[
                            gateway_data['Metric'] == 'ActiveConnectionCount'
                            ]['Maximum'].max() if 'ActiveConnectionCount' in gateway_data['Metric'].values else 0,
                        'Avg_Active_Connections': gateway_data[
                            gateway_data['Metric'] == 'ActiveConnectionCount'
                            ]['Average'].mean() if 'ActiveConnectionCount' in gateway_data['Metric'].values else 0
                    })
                if gateway_type == 'IGW':
                    vpc_id = gateway_data['VPC_ID'].iloc[0] if 'VPC_ID' in gateway_data.columns else 'Unknown'
                    metrics_summary.update({
                        'VPC_ID': vpc_id,
                        'Total_Bytes_In': gateway_data[
                            gateway_data['Metric'] == 'BytesInFromDestination'
                            ]['Sum'].sum(),
                        'Total_Bytes_Out': gateway_data[
                            gateway_data['Metric'] == 'BytesOutToDestination'
                            ]['Sum'].sum(),
                        'Total_Packets_In': gateway_data[
                            gateway_data['Metric'] == 'PacketsInFromDestination'
                            ]['Sum'].sum(),
                        'Total_Packets_Out': gateway_data[
                            gateway_data['Metric'] == 'PacketsOutToDestination'
                            ]['Sum'].sum(),
                        'Total_Blackhole_Drops_Bytes': gateway_data[
                            gateway_data['Metric'] == 'BytesDropCountBlackholeIPv4'
                            ]['Sum'].sum(),
                        'Total_NoRoute_Drops_Bytes': gateway_data[
                            gateway_data['Metric'] == 'BytesDropCountNoRouteIPv4'
                            ]['Sum'].sum(),
                        'Total_Blackhole_Drops_Packets': gateway_data[
                            gateway_data['Metric'] == 'PacketsDropCountBlackholeIPv4'
                            ]['Sum'].sum(),
                        'Total_NoRoute_Drops_Packets': gateway_data[
                            gateway_data['Metric'] == 'PacketsDropCountNoRouteIPv4'
                            ]['Sum'].sum(),
                        'Status': 'Inactive' if all(gateway_data['Sum'] == 0) else 'Active'
                    })

                # Calculate rates and percentages
                total_bytes = metrics_summary.get('Total_Bytes_In', 0) + metrics_summary.get('Total_Bytes_Out', 0)
                total_packets = metrics_summary.get('Total_Packets_In', 0) + metrics_summary.get('Total_Packets_Out', 0)

                metrics_summary.update({
                    'Total_Bytes': total_bytes,
                    'Total_Packets': total_packets,
                    'Bytes_Per_Second_Avg': round(total_bytes / (total_periods * 21600) if total_periods > 0 else 0, 2),
                    # 21600 seconds per period
                    'Packets_Per_Second_Avg': round(total_packets / (total_periods * 21600) if total_periods > 0 else 0,
                                                    2)
                })

                idle_analysis.append(metrics_summary)

        return pd.DataFrame(idle_analysis)

    idle_analysis = analyze_idle_time(df)

    # Save results to Excel with multiple sheets
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_filename = f'gateway_analysis_{account_id}_{region}_{timestamp}.xlsx'

    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        # Summary sheet
        idle_analysis_copy = idle_analysis.copy()
        # Convert any datetime columns in summary to timezone-naive
        datetime_columns = idle_analysis_copy.select_dtypes(include=['datetime64[ns, UTC]']).columns
        for col in datetime_columns:
            idle_analysis_copy[col] = idle_analysis_copy[col].dt.tz_localize(None)
        idle_analysis_copy.to_excel(writer, sheet_name='Summary', index=False)

        # Individual gateway sheets
        for gateway_id in df['Gateway_ID'].unique():
            gateway_data = df[df['Gateway_ID'] == gateway_id].copy()
            # Convert timezone-aware timestamps to timezone-naive
            gateway_data['Timestamp'] = gateway_data['Timestamp'].dt.tz_localize(None)
            gateway_name = gateway_data.iloc[0]['Gateway_Name']
            sheet_name = f"{gateway_name[:30]}"  # Truncate name if too long
            gateway_data.to_excel(writer, sheet_name=sheet_name, index=False)

        # Auto-adjust column widths
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for idx, col in enumerate(worksheet.columns, 1):
                max_length = 0
                column = get_column_letter(idx)
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column].width = min(adjusted_width, 50)

    # Print summary
    print("\nGateway Analysis Summary:")
    print("=" * 80)
    print(f"Account ID: {account_id}")
    print(f"Region: {region}")
    print("=" * 80)

    for _, row in idle_analysis.iterrows():
        print(f"\nGateway: {row['Gateway_Name']} ({row['Gateway_ID']})")
        print(f"Type: {row['Gateway_Type']}")
        print(f"VPC: {row['VPC_Name']} ({row['VPC_ID']})")
        print(f"Idle Percentage: {row['Idle_Percentage']}%")
        print(f"Total Traffic:")
        print(f"  - Bytes In: {row['Total_Bytes_In']:,}")
        print(f"  - Bytes Out: {row['Total_Bytes_Out']:,}")
        print(f"  - Packets In: {row['Total_Packets_In']:,}")
        print(f"  - Packets Out: {row['Total_Packets_Out']:,}")
        print(f"Average Rates:")
        print(f"  - Bytes/Second: {row['Bytes_Per_Second_Avg']:,.2f}")
        print(f"  - Packets/Second: {row['Packets_Per_Second_Avg']:,.2f}")

        if row['Gateway_Type'] == 'NAT':
            print("NAT Gateway Specific Metrics:")
            print(f"  - Total Connection Attempts: {row['Total_Connection_Attempts']:,}")
            print(f"  - Connection Timeouts: {row['Total_Connection_Timeouts']:,}")
            print(f"  - Port Allocation Errors: {row['Port_Allocation_Errors']:,}")
            print(f"  - Max Active Connections: {row['Max_Active_Connections']:,}")
            print(f"  - Avg Active Connections: {row['Avg_Active_Connections']:,.2f}")
        else:
            print("\nInternet Gateway Specific Metrics:")
            print(f"  - VPC ID: {row['VPC_ID']}")
            print(f"  - Status: {row['Status']}")
            print(f"  - Blackhole Drops (Bytes): {row['Total_Blackhole_Drops_Bytes']:,}")
            print(f"  - NoRoute Drops (Bytes): {row['Total_NoRoute_Drops_Bytes']:,}")
            print(f"  - Blackhole Drops (Packets): {row['Total_Blackhole_Drops_Packets']:,}")
            print(f"  - NoRoute Drops (Packets): {row['Total_NoRoute_Drops_Packets']:,}")

    print(f"\nDetailed analysis saved to: {excel_filename}")

    return df, idle_analysis


if __name__ == "__main__":
    region = 'REGION'  # Replace with your region
    profile_name = 'AWS_mfa_profile'  # Replace with your AWS profile name

    metrics_df, idle_df = get_gateway_metrics(region, profile_name)
