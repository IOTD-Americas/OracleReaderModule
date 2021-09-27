# IoT Edge Module that reads from Oracle Server using a static PL/SQL query

This module use Environmental variables, you can set them in the deployment manifest. Deployment manifest extract:

```json
"OracleReaderModule": {
	"version": "1.0",
	"type": "docker",
	"status": "running",
	"restartPolicy": "always",
	"settings": {
		"image": "myacr.azurecr.io/OracleReaderModule.amd64",
		"createOptions": {}
	},
	"env": {
		"ConnectionString": {
		"value": "User Id=YOURUSER;Password=YOURPASSW;Data Source=IPYOURSERVER:YOURPORT/YOURSID;"
		},
		"SqlQuery": {
		"value": "SELECT top 1 * FROM MyTable"
		},
		"IsSqlQueryJson": {
		"value": "false"
		},
		"PoolingIntervalMiliseconds": {
		"value": "60000"
		},
		"MaxBatchSize": {
		"value": "200"
		},
		"Verbose": {
		"value": "true"
		}
	}
}
```

"IsSqlQueryJson": true --only used when query output is json, in any other case false

Roadmap improvements

 - Secure connection string
 - Support Stored Procedures
