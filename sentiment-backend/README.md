# sentiment-backend
Agent of front-end to query mysql and return result-set.

App.kt: Entry point of this project. Launch http server and
parse request into json, send it into eventbus for further processing.

Backend.kt: Main business logic.

persistence.DBConnection.kt: Mysql connection.