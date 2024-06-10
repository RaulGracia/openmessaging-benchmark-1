import paramiko
import boto3
import time
import subprocess
from kubernetes import client, config

# Script to deploy Pravega using local NVMe drives on AWS EKS.
# To run this script, you need to perform the next steps:
#
# 1) Create the EKS cluster using eksctl:
# > eksctl create cluster --name pravega --region us-east-1 --node-type i3en.2xlarge --nodes 3 --ssh-access --ssh-public-key ~/.ssh/pravega_aws.pub
# Note that the script assumes a certain type of instance containing local drives to locate and mount them. Please, if
# you are using another type of i3 instance, set the INSTANCE_TYPE variable accordingly. It may happen that the name of
# the drives to mount change across instances. Take that into account when using the format_and_mount_nvme_drive() method.
#
# 2) Attach the AmazonEBSCSIDriverPolicy to the EKS Cluster role, so we can create EBS volumes:
# > aws iam attach-role-policy --role-name AmazonEBSCSIDriverPolicy --policy-arn arn:aws:iam::aws:policy/YourPolicyName
#
# 3) Run the deployment script for Pravega.
# > python deploy_pravega.py


def get_ec2_instances(region, filters):
    ec2 = boto3.resource('ec2', region_name=region)
    instances = ec2.instances.filter(Filters=filters)
    return instances


def format_and_mount_nvme_drive(ip_address, username, private_key_path, nvme_device, mount_directory):
    try:
        # SSH into the EC2 instance
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip_address, username=username, key_filename=private_key_path)

        # Format the NVMe drive using parted and mkfs
        format_commands = [
            f"sudo yum install -y parted",
            f"sudo parted -s /dev/{nvme_device} mklabel gpt mkpart primary ext4 0% 100%",
            f"sudo mkfs.ext4 /dev/{nvme_device}",
        ]

        for command in format_commands:
            stdin, stdout, stderr = client.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                raise Exception(f"Command execution failed with exit code {exit_status}: {stderr.read().decode()}")

        print(f"NVMe drive /dev/{nvme_device} formatted successfully on {ip_address}")

        # Mount the NVMe drive to the specified directory
        mount_command = f"sudo mkdir -p {mount_directory} && sudo mount /dev/{nvme_device} {mount_directory}"
        stdin, stdout, stderr = client.exec_command(mount_command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            raise Exception(f"Mounting failed with exit code {exit_status}: {stderr.read().decode()}")

        print(f"NVMe drive /dev/{nvme_device} mounted to {mount_directory} on {ip_address}")

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Close the SSH connection
        client.close()


def run_command(tool, command):
    try:
        # Run command and capture the output
        result = subprocess.run([tool] + command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Print the command output
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        # Print error output if the command fails
        print(f"Error: {e.stderr}")


def create_persistent_volume(pv_name, bookie_path):
    # Load Kubernetes configuration from default location or kubeconfig file
    config.load_kube_config()

    # Create a PV object
    pv = client.V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata=client.V1ObjectMeta(name=pv_name),
        spec=client.V1PersistentVolumeSpec(
            capacity={"storage": "1000Gi"},
            volume_mode="Filesystem",
            access_modes=["ReadWriteOnce"],
            persistent_volume_reclaim_policy="Retain",
            storage_class_name="local-storage",
            local=client.V1LocalVolumeSource(path=bookie_path),
            node_affinity=client.V1VolumeNodeAffinity(
                required=client.V1NodeSelector(
                    node_selector_terms=[
                        client.V1NodeSelectorTerm(
                            match_expressions=[
                                client.V1NodeSelectorRequirement(
                                    key="node.kubernetes.io/instance-type",
                                    operator="In",
                                    values=["i3en.2xlarge"]
                                )
                            ]
                        )
                    ]
                )
            )
        )
    )

    # Create a Kubernetes API client
    api_instance = client.CoreV1Api()

    try:
        # Create the PV
        api_instance.create_persistent_volume(body=pv)
        print("Persistent Volume created successfully.")

    except client.rest.ApiException as e:
        print(f"Exception when calling CoreV1Api->create_persistent_volume: {e}")


def set_local_drives_rancher():
    configmap_name = "local-path-config"
    namespace = "local-path-storage"

    # Load the kubeconfig file or use the in-cluster configuration
    config.load_kube_config()
    # Create a Kubernetes client
    v1 = client.CoreV1Api()
    try:
        # Retrieve the existing ConfigMap
        configmap = v1.read_namespaced_config_map(name=configmap_name, namespace=namespace)
        # Update the data field with the new value
        print(configmap.data['config.json'])
        configmap.data['config.json'] = configmap.data['config.json'].replace('\"/opt/local-path-provisioner\"', '\"/var/lib/kafka/\"')
        print(configmap.data['config.json'])
        # Patch the ConfigMap with the updated data
        v1.patch_namespaced_config_map(name=configmap_name, namespace=namespace, body=configmap)
        print(f"ConfigMap '{configmap_name}' in namespace '{namespace}' updated successfully.")
    except Exception as e:
        print(f"Error updating ConfigMap: {str(e)}")

def main():
    # AWS credentials and region
    aws_access_key = 'access_key'
    aws_secret_key = 'secret_key'
    aws_region = 'us-east-1'

    # EC2 filters (modify as needed)
    filters = [
        {'Name': 'instance-state-name', 'Values': ['running']},
        # Add more filters if needed
    ]

    # Replace these values with your EC2 instance details
    username = 'ec2-user'
    private_key_path = '/home/raul/.ssh/pravega_aws.pub'
    journal_nvme_device = 'nvme1n1'
    journal_mount_directory = '/var/lib/kafka'
    #ledger_nvme_device = 'nvme2n1'
    #ledger_mount_directory = '/home/ledger'

    try:
        # Create storage classes.
        run_command("kubectl", ["create", "-f", "./storage-classes.yaml"])

        # Format node drives, mount them, and create the PVs for Bookies.
        ec2_instances = get_ec2_instances(aws_region, filters)
        for instance in ec2_instances:
            ip_address = instance.public_ip_address
            format_and_mount_nvme_drive(ip_address, username, private_key_path, journal_nvme_device, journal_mount_directory)
            # Add a delay between instances (optional)
            time.sleep(1)

        # Create kafka namespace
        run_command("kubectl", ["create", "namespace", "kafka"])

        # Install Rancher local volume provisioner.
        run_command("kubectl", ["apply", "-f", "https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml"])
        time.sleep(5)
        set_local_drives_rancher()

        # Install the EBS volume provisioner.
        run_command("kubectl", ["apply", "-k", "github.com/kubernetes-sigs/aws-ebs-csi-driver/deploy/kubernetes/overlays/stable/?ref=master"])

        # Install Kafka Operator.
        run_command("kubectl", ["apply", "-f", "https://strimzi.io/install/latest?namespace=kafka", "-n", "kafka"])
        time.sleep(10)

        # Install Zookeeper and Kafka.
        run_command("kubectl", ["create", "-f", "./kafka.yaml", "-n", "kafka"])


    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()

