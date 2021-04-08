# Leaderless Replication


## Usage

First, configure your settings in the YAML file.

Example of starting a server manually:

```bash
go run server/server.go 0 --disk
```

* The first command line argument is the ID of the server you want to start.

* The second is a flag that determines if you want your data to be stored on the disk or not. Options: --memory or --disk

Example of starting a client manually:

```bash
go run client/client.go
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)