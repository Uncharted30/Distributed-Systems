package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

// read intermediate key and values from json file and call reduce function
// then save final results to mr-out-* files
func DoReduce(mapTasks int, taskId int, reducef func(string, []string) string) bool {
	filename := "mr-out-" + strconv.Itoa(taskId)
	file, res := createTmpFile("reduce")
	if !res {
		return false
	}

	var kva []KeyValue

	for i := 0; i < mapTasks; i++ {
		kva, _ = decodeJson(i, taskId, kva)
	}

	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		result := reducef(key, values)

		_, err := fmt.Fprintf(file, "%v %v\n", key, result)
		if err != nil {
			log.Printf("Reduce task %d: error output result.\n", taskId)
			return false
		}

		i = j
	}

	if fileExists(filename) {
		return false
	}

	res = renameFile(file.Name(), filename)
	return res
}

// decode json files
func decodeJson(index int, taskId int, kva []KeyValue) ([]KeyValue, bool) {
	filename := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(taskId)
	file, err := os.Open(filename)
	if err != nil {
		log.Println("File " + filename + " does not exits.")
		return kva, false
	}

	dec := json.NewDecoder(file)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva, true
}