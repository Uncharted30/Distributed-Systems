## Describe a sequence of events that would result in a client reading stale data from the Google File System.

I can think of two sequence of events.

The first one happens when a chunkserver recovers from a faliure:
1. One client asks master for locations of some chunks, and the master replies with the locations of replicas.

2. Shortly after client got the locations of replicas, some other clients write some data to these same chunks. And one of the chunkserver that stores these chunks failed.

3. The failed chunkserver restarts, but it has missed some mutation request.

4. Locations of replicas in stored in the client's cache, the client selects a replica, and accidentally, the client selected the chunkserver that failed during the mutation. So the client would read some stale data.

The second one happens when the master fails, and at the same time, one of the chunkservers fails and missed some mutation request. Shortly after the master fails, one client send a request to master to read one chunk, the request goes to the shadow master. But the shadow server has the older chunk version number. When the client select the chuckserver that failed during the mutation mentioned above, the version number client got from the shadow master will be the same with the version number on that chunkserver, but the data on that chunkserver is stale data.