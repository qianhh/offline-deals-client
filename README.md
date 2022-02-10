The "offline-deals-client" connects the offline-deals service, downloads and imports deal data to lotus-miner.

# Requirements
1. Get an offline-deals account (https://datasets.filedrive.io)
2. Depending of your setup :
   - Monolitic lotus : deploy offline-deals-client service on your miner only (if you don't know what is split store, you probably run a Monolitic lotus)
   - Split Store lotus : deploy offline-deals-client service on all markets node instead of the miner
3. Ensure your offline deals switch to on:
```
lotus-miner storage-deals selection list
```
if considering offline storage deals is false, then reset it:
```
lotus-miner storage-deals selection reset
```
4. Install golang 1.16 or later

# Get Started
1. Install the offline-deals-client
```
git clone https://github.com/filedrive-team/offline-deals-client.git
cd offline-deals-client
make
```
2. Add your offline-deals authentification TOKEN
```
cd conf
vi ./app.toml
```
3. Run offline-deals-client
```
./offline-deals-client daemon
```

DONE :) you can now enjoy offline-deals