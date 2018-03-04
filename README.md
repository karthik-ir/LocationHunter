### Location Tracker

Run the project using  `go install ./main/`

Run the test coverage with `go test -coverprofile=main`

##### Project walk through 

* Entry Point : `LocationHunter.go`

* Heap implementation : `Heap.go` using "container/heap" 

* Distance Calculation: `DistanceCalculator.go`

* Test case file: `location_hunter_test.go`

* Interface `Datasource` is used to maintain consistency between custom implementations. 
Two of sample implementations are as below :

1. CSV processing Implementation: `FileProcessing.go`

2. Mysql processing Implementation: `DatabaseProcessing.go`

##### Commandline arguments 

1. `datasource` - use this to toggle between multiple implementations.
                  For now its either `file` and `db`. Defaulting to `file`

2. File properties
    
    a. `file` - Absolute path of the file
    
    b. `separator` - The csv seprator. By default `,`
    
    c. `strict` - `true` or `false` if false, bad lines in the file in between are ignored and processed further.

3. DB Properties

    a. `dbconnectionstring` - Defaults to `localhost:3306` used to connect to the database.
    
    b. `user` - username of the database
    
    c. `password` - password to connect to the datasource
    
    d. `database` - actual database name. 
    
    e. `table` - Table name defaulting to `geoData`
    
4. Common properties

    a. `lat` - Home latitude location to be searched with. Defaulting to `51.925146`
    
    b. `lng` - Home Longitude location. Defaulting to `4.478617`
    
    c. `top` - Search top `n` nearest location. Defaulting to `5`

### Other notes:

The project uses `n` sized heaps in order to be able to calculate large data sets.

The top `n` nearest locations are displayed in the console at the end of the program. 

#sample output#

```$xslt
Number: 1 Distance 0.33 Data: {51.9271671 4.4822171 442406} 

Number: 2 Distance 0.53 Data: {51.9253559 4.4863098 285782} 

Number: 3 Distance 0.65 Data: {51.92562969999999 4.4880344 429151} 

Number: 4 Distance 0.74 Data: {51.9268152 4.489072 512818} 

Number: 5 Distance 0.82 Data: {51.92491219999999 4.490593 25182} 
```


*Assumptions* 

1. Database columns are id,lat,lng .
2. Distance displayed in kilometers are the Earth radius is considered as `6371`, not taking in the other 
variables in consideration.