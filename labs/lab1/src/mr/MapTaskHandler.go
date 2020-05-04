package mr

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

// do map and save intermediate key values to json files
func DoMap(filename string, taskId int, reduceTasks int, mapf func(string, string) []KeyValue) bool {
	log.Println(filename)
	content := readFile(filename)
	kva := mapf(filename, content)
	sort.Sort(ByKey(kva))
	intermediateMap := make(map[int][][]KeyValue)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var keyValues []KeyValue
		for k := i; k < j; k++ {
			keyValues = append(keyValues, kva[i])
		}

		reduceId := ihash(key) % reduceTasks
		intermediateMap[reduceId] = append(intermediateMap[reduceId], keyValues)

		i = j
	}

	result := toJson(taskId, intermediateMap)
	return result
}

// read input file
func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	return string(content)
}

// save intermediate key and values to json files
func toJson(taskId int, intermediateMap map[int][][]KeyValue) bool {
	for key := range intermediateMap {
		filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(key)


		file, res := createTmpFile("map")

		if !res {
			return false
		}

		enc := json.NewEncoder(file)
		for _, keyValues := range intermediateMap[key] {
			for _, keyValue := range keyValues {
				err := enc.Encode(keyValue)
				if err != nil {
					log.Println("Cannot encode key value: " + keyValue.Key + keyValue.Value + " to Json")
				}
			}
		}

		if fileExists(filename) {
			continue
		}

		res = renameFile(file.Name(), filename)

		if !res {
			return false
		}

		_ = file.Close()
	}

	return true
}