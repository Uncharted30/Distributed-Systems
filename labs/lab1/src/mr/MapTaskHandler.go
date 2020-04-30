package mr

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

func doMap(filename string, taskId int, reduceTasks int, mapf func(string, string) []KeyValue) bool {
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

func toJson(taskId int, intermediateMap map[int][][]KeyValue) bool {
	for key := range intermediateMap {
		filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(key)

		file, err := ioutil.TempFile("", "tmp-*")
		if err != nil {
			log.Println("Error creating temp file.")
			return false
		}

		enc := json.NewEncoder(file)
		for _, keyValues := range intermediateMap[key] {
			for _, keyValue := range keyValues {
				err := enc.Encode(keyValue)
				if err != nil {
					log.Println("Cannot decode key value: " + keyValue.Key + keyValue.Value + " to Json")
				}
			}
		}

		_, err = os.Stat(filename)

		if err == nil || !os.IsNotExist(err) {
			return false
		}

		err = os.Rename(file.Name(), filename)
		if err != nil {
			_ = os.Remove(file.Name())
			log.Println("Failed to rename file")
			return false
		}

		_ = file.Close()
	}

	return true
}