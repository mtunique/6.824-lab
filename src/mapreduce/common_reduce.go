package mapreduce

import (
	"os"
	"encoding/json"
	"io"
	"log"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//


	var _, err = os.Stat(outFile)
	// create file if not exists
	if os.IsNotExist(err) {
		var file, _ = os.Create(outFile)
		file.Close()
	}

	file, err := os.OpenFile(
		outFile,
		os.O_WRONLY,
		0600)

	defer file.Close()

	enc := json.NewEncoder(file)

	var kvs []KeyValue

	for m := 0; m < nMap; m++  {

		reduceFileName := reduceName(jobName, m, reduceTaskNumber)
		reduceFile, err := os.OpenFile(
			reduceFileName,
			os.O_RDONLY,
			0600)

		if err != nil {
			log.Fatal("Open file", reduceFileName, "error ", err)
		}

		dec := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			kvs = append(kvs, kv)
		}

		reduceFile.Close()
	}

	sort.Sort(KeyValues(kvs))

	start := 0
	preKey := ""

	if len(kvs) > 0 {
		preKey = kvs[0].Key
	}

	for index, kv := range kvs {
		if preKey != kv.Key {
			result := reduceF(preKey, getValue(kvs, start, index))
			enc.Encode(KeyValue{preKey, result})

			preKey = kv.Key
			start = index
		}
	}

	if start < len(kvs) {
		result := reduceF(preKey, getValue(kvs, start, len(kvs)))
		enc.Encode(KeyValue{preKey, result})
	}


}

func getValue(kvs []KeyValue, start int, end int) (values []string) {
	for _, tmpKv := range kvs[start:end] {
		values = append(values, tmpKv.Value)
	}
	return
}
