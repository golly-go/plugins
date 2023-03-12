# ORM
Golly ORM comes prepackaged with the gorm for information about gorm visit there documentation page
[GORM](https://gorm.io). Some or all of the default configuration can be overriden. On top of the default configuration Golly accepts a connection URL wiht the following ENV `DATABASE_URL` 

#### Usage
To use the ORM plugin add orm.Initializer to golly's initializer by default located in `app/initializers/intializers.go`. Once ORM is initialized you can get a connection with `orm.DB(ctx)` the context provided to ORM.

For a global connection you can use `orm.Connection()` though this does not initialize a unique session and must be managed seperately.


#### Default Configuration

	v.SetDefault(appName, map[string]interface{}{
		"db": map[string]interface{}{
			"host":     "127.0.0.1",
			"port":     "5432",
			"username": "app",
			"password": "password",
			"name":     appName,
			"driver":   "postgres",
		},
	})

