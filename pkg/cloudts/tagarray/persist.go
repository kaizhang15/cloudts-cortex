package tagarray

import (
	"encoding/gob"
	"os"
	"path/filepath"
)

func (ta *TagArray) Save(path string) error {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	file, err := os.Create(filepath.Join(path, "tagarray.bin"))
	if err != nil {
		return err
	}
	defer file.Close()

	return gob.NewEncoder(file).Encode(ta)
}

func Load(path string) (*TagArray, error) {
	file, err := os.Open(filepath.Join(path, "tagarray.bin"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var ta TagArray
	if err := gob.NewDecoder(file).Decode(&ta); err != nil {
		return nil, err
	}
	return &ta, nil
}