This repository tries to verify leveldb batchwrite.

Conclusion:
1. Everything inside batchwrite is all-or-noghting
2. Delete operation in leveldb is adding additional record
    Delete can also be guranted
3. Leveldb will try to repair next time db start
