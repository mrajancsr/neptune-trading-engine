# Phobos Exchange Connectivity

### Setup
* `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
* `brew install redis` or if on an M1 Mac `arch -arm64 brew install redis`
* `brew services restart redis`
* `pip install -r requirements.txt`
* ensure the environment variable `PHOBOSTRADINGENGINEPATH` exists and points to where this repo is cloned to
* ensure the environment variable `PHOBOSSQLCONFIGPATH` exists and points to where the previous command installed `phobos-db`

### Starting an Exchange Feed
*`python3 feed_ingestor_to_redis.py -t 'ADA/USD' -n 'kraken-L1'`
* If you want the output to be persisted to TimescaleDB, add the flag `--save_stream`, or alternatively for shorthand `-s`. Otherwise, the output will not be persisted to TimescaleDB by default.

### Notes on persistence in redis
* When flag `--save_stream`, or alternatively for shorthand `-s`, is added, make sure to create the corresponding redis stream in your local machine.  Otherwise the program won't execute
* Go to your terminal and type `redis-cli` and then type `xadd <stream_name> * price 1;`.  You will see a unique id associated with the value
you just created echoed on the screen. Copy this value and type `xdel <stream_name> unique_id` where you paste the id you copied in place of unique_id
* Now you have a local redis stream thats ready to save raw streams

### Prefect Orion Deployment Instructions
* To deploy using prefect orion, please install the latest version of prefect by going to your terminal and typing 
`pip install -U "prefect>=2.0a"` which installs the alpha version of prefect
* On terminal, type `prefect work-queue create <work_queue_name>`.  This will create a active work queue.  You will see an work queue id
echoed on the screen.  Copy this work queue id and type the following: `prefect agent start <work_queue_name>`
* Open another terminal and type the following: `prefect orion start`
* Now you have the orion running on your lost host.  Go to the url `http://127.0.0.1:4200/` on your browser to see the orion dashboard
* Specific deployment instructions for orion can be found here `https://orion-docs.prefect.io/tutorials/deployments/`

### AWS Debugging (what has gone wrong so far)
#### What made my code crash?
* `cd /var/log`
* `cat syslog | grep python`
* `cat syslog | grep "Out of memory"`
#### How many processes is redis running?
* `ps aux | grep redis | wc -l` (sub any cli word for redis)
#### What are the redis processes running?
* `ps aux | grep redis`
#### How is my memory distributed across my cores?
* `htop`
#### What are my top memory/CPU consuming processes (realtime)?
* `top`
#### How to check if memory is getting cleaned properly in redis?
* `sudo tail -f /var/mail/ubuntu`
