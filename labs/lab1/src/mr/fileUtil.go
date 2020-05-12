package mr

import (
	"io/ioutil"
	"log"
	"os"
)

// determine if the file exists
// if the file exists, the task has been done by another worker
func fileExists(filename string) bool {
	_, err := os.Stat(filename)

	if err == nil || !os.IsNotExist(err) {
		return true
	}

	return false
}

// rename tmp file to final file
func renameFile(oldName string, newName string) bool {
	err := os.Rename(oldName, newName)
	if err != nil {
		_ = os.Remove(oldName)
		log.Println("Failed to rename file")
		return false
	}
	return true
}

// creates temp file of given pattern
func createTmpFile(pattern string) (*os.File, bool) {
	file, err := ioutil.TempFile("", "tmp-"+pattern+"-*")
	if err != nil {
		log.Println("Error creating temp file.")
		return nil, false
	}
	return file, true
}
