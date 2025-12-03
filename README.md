# Paxos Implementation
#

## Prerequisites
- **Java 21** (Gradle toolchain will use it)
- macOS/Linux terminal
- No manual protobuf setup needed (Gradle generates gRPC/protobuf code)

## Build
```bash
./gradlew clean build
```

## Start the nodes (5 terminals)
Define the peer list once (optional):
```bash
PEERS="n1:localhost:60691,n2:localhost:60692,n3:localhost:60693,n4:localhost:60694,n5:localhost:60695"
```
Then, in five separate terminals, run:
```bash
# Terminal 1
./gradlew -q run --args "--id n1 --port 60691 --peers $PEERS"

# Terminal 2
./gradlew -q run --args "--id n2 --port 60692 --peers $PEERS"

# Terminal 3
./gradlew -q run --args "--id n3 --port 60693 --peers $PEERS"

# Terminal 4
./gradlew -q run --args "--id n4 --port 60694 --peers $PEERS"

# Terminal 5
./gradlew -q run --args "--id n5 --port 60695 --peers $PEERS"
```

Each node terminal also accepts interactive commands:
- printdb | printlog | printview | printstatusall | printstatus <seq> | sleep | wake | exit

## Run the client (drives the test sets)
In a new terminal, after all nodes are running:
```bash
# Option A: Use classpath printed by Gradle
java -cp "$(./gradlew -q printRuntimeClasspath)" paxos.Client.ClientMain CSE535-F25-Project-1-Testcases.csv

# Option B: omit CSV path to use default
java -cp "$(./gradlew -q printRuntimeClasspath)" paxos.Client.ClientMain
```

Client REPL commands (type in the client terminal):
- printlog nX | printdb nX | printstatus <seq> | printview | sleep nX <ms> | wake nX | printall | next | exit

During execution, you will see lines like:
- `=== START SET <N> ... ===`
- `=== END SET <N> (timers paused for presentation) ===`
Type `next` in the client to continue to the next set.

## Stopping nodes
If ports are busy or you want to clean up:
```bash
bash kill_nodes.sh
```

## Troubleshooting
- If a node fails to start due to port conflicts, run `bash kill_nodes.sh` and retry.
- Ensure Java 21 is selected (Gradle uses the configured toolchain).
