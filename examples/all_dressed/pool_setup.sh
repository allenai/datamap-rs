#!/bin/bash 

# Set up raid drives
sudo yum install mdadm -y
NUM_DRIVES=$(echo "$(ls /dev/nvme*n1 | wc -l) - 1" | bc)
MDADM_CMD="sudo mdadm --create /dev/md0 --level=0 --raid-devices=$NUM_DRIVES"
for i in $(seq 1 $NUM_DRIVES); do
  MDADM_CMD="$MDADM_CMD /dev/nvme${i}n1"
done

eval $MDADM_CMD
sudo mkfs.xfs /dev/md0
sudo mkdir /mnt/raid0
sudo mount /dev/md0 /mnt/raid0
sudo chown -R $USER /mnt/raid0


# Download and set up all packages we need 
sudo yum install gcc -y
sudo yum install cmake -y
sudo yum install openssl-devel -y
sudo yum install g++ -y
sudo yum install htop -y
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz 
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz 
sudo mv s5cmd /usr/local/bin
sudo yum install git -y 
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh
bash rustup.sh -y
source ~/.bashrc


# Do datamap-rs setups 
cd
git clone https://github.com/revbucket/datamap-rs.git
cd datamap-rs
git checkout dclm
s5cmd run examples/all_dressed/s5cmd_asset_downloader.txt
cargo build --release 


# Do minhash-rs setups 
cd
git clone https://github.com/revbucket/minhash-rs.git
cd minhash-rs
git checkout refac2025
cargo build --release 




