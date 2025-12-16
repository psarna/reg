# reg: OCI registry you can actually query

```
                              
 $$$$$$\   $$$$$$\   $$$$$$\  
$$  __$$\ $$  __$$\ $$  __$$\ 
$$ |  \__|$$$$$$$$ |$$ /  $$ |
$$ |      $$   ____|$$ |  $$ |
$$ |      \$$$$$$$\ \$$$$$$$ |
\__|       \_______| \____$$ |
                    $$\   $$ |
                    \$$$$$$  |
                     \______/ 
```

My humble attempt at implementing a minimum viable product for an OCI registry that you can actually query.
The official implementation makes it notoriously hard to efficiently get a list of all repositories, all blobs, all manifests, all tags, etc.
This implementation's goals are as follows:
1. Work with existing S3-backed format, so that you can instantly query any existing registry backend.
2. Keep metadata information (manifests, configs, tags) in a local SQLite database.
3. Add an optional bootstrapping step which scrapes any existing registry and gathers all information in SQLite.
4. As long as you only use a single instance of this registry for pushing, all data is trivially up-to-date,
   allowing you to easily list all kinds of information, but also efficiently garbage-collect unused blobs.
