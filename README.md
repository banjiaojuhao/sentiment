# sentiment
## sub-projects:
- sentiment-backend: backend of data visualization.
- sentiment-spider: spider for sentiment data.
- sentiment-persistence: tables definition of mysql.sentiment.
- sentiment-distribution: distribute tasks to other hosts for words splitting and classification.


## future todo:
- annotation to mark backend request handler
- proxy pool service: 
Written in golang. Use MITM to resend request through proxies. 
User can evaluate proxy according to response(status code, response body).
- log: save log into SQLite for convenient query and delete outdated.
- task management: manage task(spider, restore) and checked error msg
 by status, error_table and timestamp like sentiment-distribution.
> ###related problems:
> - tieba and zhihu empty username
> - partial sync failure(RestoreVerticle.kt)
> - retry fetching zhihu deleted question 
- verticle: deploy and undeploy function return after service is ready.
