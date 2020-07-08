# sentiment
## sub-projects:
- sentiment-backend: backend of data visualization.
- sentiment-spider: spider for sentiment data.
- sentiment-persistence: tables definition of mysql.sentiment.
- sentiment-distribution: distribute tasks to other hosts for words splitting and classification.


## future todo:
- annotation to mark backend request handler
- proxy pool service: 
Written in golang. Use MITM to transfer request through proxies. 
User can evaluate proxy according to response(status code, response body).
- log: save log into SQLite for convenient query and delete outdated.
- task management: manage task(spider, restore) and checked error msg
 by status, error_table and timestamp like sentiment-distribution.
> ###related problems:
> - tieba and zhihu empty username
> - partial sync failure(RestoreVerticle.kt)
> - retry fetching zhihu deleted question 
- verticle: deploy and undeploy function return after service is ready.
- distribution: transfer over tls.


# Build and deployment:
> add mirror for gradle:
>
> download [huawei cloud gradle configure](https://mirrors.huaweicloud.com/v1/configurations/gradle) into C:\Users\\\<UserName>\\.gradle\init.gradle

POWERSHELL> .\gradlew.bat installDist

wsl> rsync -avz \*/build/install/\*/\* user@remote-host:path/to/deploy

## Run:
linux> mkdir <project-name>(eg: sentiment-spider) && cd <project-name>

linux> path/to/deploy/bin/<project-name>

### or run on windows without deployment:

windows>  <project-name>\build\install\\<project-name>\bin\\<project-name>.bat