### API-server
Example local build cmd: 
```bash 
docker build -f Dockerfile -t nlsql-api-server . 
```
Example local run cmd: 
```bash
docker run --rm -p 80:80 --env-file .env nlsql-api-server 
```

Docker env params:
  * DatabaseType (*Options: 'mysql', 'mssql', 'snowflake', 'redshift', 'postgresql', 'bigquery'*)
  * DataSource (*For 'snowflake' analogue of the 'Account' db parameter*)
  * Warehouse (*Used when DataSource='snowflake'*)
  * DbSchema (*Used when DataSource='snowflake'*)
  * DbName
  * DbUser
  * DbPassword
  * DbPort (*Used when DataSource is 'mysql' or 'postgresql'*)
  * ApiEndPoint
  * ApiToken
  * AppId
  * AuthTenantID (*Optional*)
  * AppPassword
  * StaticEndPoint
  * client_email (*Used when DataSource='bigquery'*)
  * token_uri (*Used when DataSource='bigquery'*)
  * private_key (*Used when DataSource='bigquery'*)
  * project_id (*Used when DataSource='bigquery'*)
  

API endpoint: ```/nlsql-analyzer```

Method: ```POST```

JSON: ```{"channel_id": str, "text": str}```

### Nginx
location ```~* \.(jpg|jpeg|gif|png|css|zip|tgz|gz|rar|bz2|doc|xls|exe|pdf|ppt|tar|mid|midi|wav|bmp|rtf|js|swf|docx|xlsx|svg|csv|html)$```
to ```root /var/www/html```
 
location ```/api/messages```
to ```proxy_pass http://localhost:8000```